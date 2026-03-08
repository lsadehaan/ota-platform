package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type scenario struct {
	Cards          int
	Planners       int
	Executors      int
	Gateways       int
	Projectors     int
	ExpectResponse bool
}

type result struct {
	Scenario  scenario
	Elapsed   string
	CardTPS   float64
	PartTPS   float64
	Sent      int64
	Delivered int64
	Failed    int64
	Status    string
	Progress  float64
}

var summaryLine = regexp.MustCompile(`E2E summary: cards=([0-9]+) elapsed=([^ ]+) card_tps=([0-9.]+) sms_part_tps=([0-9.]+) sent=([0-9]+) delivered=([0-9]+) failed=([0-9]+) status=([^ ]+) progress=([0-9.]+)%`)

func main() {
	var (
		composeFiles   = flag.String("compose-files", "deployments/docker-compose.yml,deployments/docker-compose.load.yml", "comma-separated docker compose files")
		cardList       = flag.String("cards", "50,200", "comma-separated card counts")
		plannerList    = flag.String("planners", "1,2", "comma-separated planner replica counts")
		executorList   = flag.String("executors", "1,2", "comma-separated executor replica counts")
		gatewayList    = flag.String("gateways", "1,2", "comma-separated gateway replica counts")
		projectorList  = flag.String("projectors", "1", "comma-separated projector replica counts")
		expectResponse = flag.Bool("expect-response", true, "whether the campaign step waits for a PoR/MO response")
		networkName    = flag.String("network", "deployments_default", "compose network name for e2e test container")
		campaignTO     = flag.String("campaign-timeout", "2m", "campaign completion timeout passed to the E2E test")
		build          = flag.Bool("build", false, "rebuild service images before running scenarios")
		keepStack      = flag.Bool("keep-stack", false, "leave the last scenario stack running")
		output         = flag.String("output", "", "optional markdown report output path")
	)
	flag.Parse()

	scenarios, err := buildScenarios(*cardList, *plannerList, *executorList, *gatewayList, *projectorList, *expectResponse)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build scenarios: %v\n", err)
		os.Exit(1)
	}
	files := splitCSV(*composeFiles)
	results := make([]result, 0, len(scenarios))

	for i, sc := range scenarios {
		if err := composeDown(files); err != nil {
			fmt.Fprintf(os.Stderr, "compose down before scenario %d: %v\n", i+1, err)
			os.Exit(1)
		}
		if err := composeUp(files, sc, *build); err != nil {
			fmt.Fprintf(os.Stderr, "compose up for scenario %d: %v\n", i+1, err)
			os.Exit(1)
		}
		res, err := runScenario(files, *networkName, *campaignTO, sc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "scenario %d failed: %v\n", i+1, err)
			os.Exit(1)
		}
		results = append(results, res)
	}

	if !*keepStack {
		if err := composeDown(files); err != nil {
			fmt.Fprintf(os.Stderr, "compose down after scenarios: %v\n", err)
			os.Exit(1)
		}
	}

	report := renderReport(results, files)
	if *output != "" {
		if err := os.WriteFile(*output, []byte(report), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write report: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Print(report)
}

func buildScenarios(cardsCSV, plannersCSV, executorsCSV, gatewaysCSV, projectorsCSV string, expectResponse bool) ([]scenario, error) {
	cards, err := parseCSVInts(cardsCSV)
	if err != nil {
		return nil, fmt.Errorf("cards: %w", err)
	}
	planners, err := parseCSVInts(plannersCSV)
	if err != nil {
		return nil, fmt.Errorf("planners: %w", err)
	}
	executors, err := parseCSVInts(executorsCSV)
	if err != nil {
		return nil, fmt.Errorf("executors: %w", err)
	}
	gateways, err := parseCSVInts(gatewaysCSV)
	if err != nil {
		return nil, fmt.Errorf("gateways: %w", err)
	}
	projectors, err := parseCSVInts(projectorsCSV)
	if err != nil {
		return nil, fmt.Errorf("projectors: %w", err)
	}
	out := make([]scenario, 0, len(cards)*len(planners)*len(executors)*len(gateways)*len(projectors))
	for _, c := range cards {
		for _, p := range planners {
			for _, e := range executors {
				for _, g := range gateways {
					for _, r := range projectors {
						out = append(out, scenario{Cards: c, Planners: p, Executors: e, Gateways: g, Projectors: r, ExpectResponse: expectResponse})
					}
				}
			}
		}
	}
	return out, nil
}

func composeUp(files []string, sc scenario, build bool) error {
	args := composeArgs(files)
	args = append(args, "up", "-d")
	if build {
		args = append(args, "--build")
	}
	args = append(args,
		"--scale", fmt.Sprintf("campaign-planner=%d", sc.Planners),
		"--scale", fmt.Sprintf("card-executor=%d", sc.Executors),
		"--scale", fmt.Sprintf("sms-gateway=%d", sc.Gateways),
		"--scale", fmt.Sprintf("read-model-projector=%d", sc.Projectors),
	)
	cmd := exec.Command("docker", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func composeDown(files []string) error {
	args := composeArgs(files)
	args = append(args, "down", "-v", "--remove-orphans")
	cmd := exec.Command("docker", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func composeArgs(files []string) []string {
	args := []string{"compose"}
	for _, file := range files {
		args = append(args, "-f", file)
	}
	return args
}

func runScenario(files []string, networkName, campaignTimeout string, sc scenario) (result, error) {
	cmd := exec.Command("docker", "run", "--rm",
		"--network", networkName,
		"-v", fmt.Sprintf("%s:/workspace", mustGetwd()),
		"-w", "/workspace",
		"-e", "E2E_BASE_URL=http://ota-api:8080",
		"-e", fmt.Sprintf("E2E_CARD_COUNT=%d", sc.Cards),
		"-e", fmt.Sprintf("E2E_CAMPAIGN_TIMEOUT=%s", campaignTimeout),
		"-e", fmt.Sprintf("E2E_EXPECT_RESPONSE=%t", sc.ExpectResponse),
		"golang:1.26-alpine",
		"sh", "-lc", "export PATH=/usr/local/go/bin:$PATH && apk add --no-cache git >/dev/null && GOCACHE=/tmp/go-build go test -tags=e2e ./e2e -v -count=1",
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return result{}, fmt.Errorf("%w: %s%s", err, stdout.String(), stderr.String())
	}
	res, err := parseSummary(stdout.Bytes())
	if err != nil {
		return result{}, fmt.Errorf("parse e2e summary: %w\n%s", err, stdout.String())
	}
	res.Scenario = sc
	return res, nil
}

func parseSummary(output []byte) (result, error) {
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		match := summaryLine.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		cardTPS, _ := strconv.ParseFloat(match[3], 64)
		partTPS, _ := strconv.ParseFloat(match[4], 64)
		sent, _ := strconv.ParseInt(match[5], 10, 64)
		delivered, _ := strconv.ParseInt(match[6], 10, 64)
		failed, _ := strconv.ParseInt(match[7], 10, 64)
		progress, _ := strconv.ParseFloat(match[9], 64)
		return result{
			Elapsed:   match[2],
			CardTPS:   cardTPS,
			PartTPS:   partTPS,
			Sent:      sent,
			Delivered: delivered,
			Failed:    failed,
			Status:    match[8],
			Progress:  progress,
		}, nil
	}
	return result{}, fmt.Errorf("summary line not found")
}

func renderReport(results []result, files []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# E2E Load Report\n\n")
	fmt.Fprintf(&b, "Generated: %s UTC\n\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "- Compose files: `%s`\n\n", strings.Join(files, ", "))
	fmt.Fprintf(&b, "| Cards | Planners | Executors | Gateways | Projectors | Expect Response | Elapsed | Card TPS | SMS Part TPS | Sent | Delivered | Failed | Status | Progress |\n")
	fmt.Fprintf(&b, "| ---: | ---: | ---: | ---: | ---: | :---: | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: |\n")
	for _, res := range results {
		fmt.Fprintf(&b, "| %d | %d | %d | %d | %d | %t | %s | %.2f | %.2f | %d | %d | %d | %s | %.2f%% |\n",
			res.Scenario.Cards, res.Scenario.Planners, res.Scenario.Executors, res.Scenario.Gateways, res.Scenario.Projectors,
			res.Scenario.ExpectResponse, res.Elapsed, res.CardTPS, res.PartTPS, res.Sent, res.Delivered, res.Failed, res.Status, res.Progress)
	}
	return b.String()
}

func parseCSVInts(raw string) ([]int, error) {
	parts := splitCSV(raw)
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		v, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", part, err)
		}
		out = append(out, v)
	}
	return out, nil
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func mustGetwd() string {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return wd
}
