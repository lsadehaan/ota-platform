package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"time"
)

type benchmarkSuite struct {
	Name    string
	Package string
	Regex   string
}

type benchmarkResult struct {
	Suite       string
	Package     string
	Benchmark   string
	Iterations  string
	NsPerOp     string
	BPerOp      string
	AllocsPerOp string
}

var benchmarkLine = regexp.MustCompile(`^(Benchmark\S+)\s+([0-9]+)\s+([0-9.]+\s+ns/op)(?:\s+([0-9.]+\s+B/op))?(?:\s+([0-9.]+\s+allocs/op))?$`)

func main() {
	var (
		benchtime = flag.String("benchtime", "3x", "go test -benchtime value")
		count     = flag.Int("count", 1, "go test -count value")
		output    = flag.String("output", "", "optional markdown report output path")
	)
	flag.Parse()

	suites := []benchmarkSuite{
		{Name: "Campaign Planning", Package: "./internal/controlplane", Regex: "BenchmarkStartCampaignShardPlanning"},
		{Name: "Shard Publishing", Package: "./internal/planner", Regex: "BenchmarkPublishClaimedShards"},
		{Name: "Activation Pipeline", Package: "./internal/planner", Regex: "BenchmarkActivation(Pipeline|ResponsePipeline)$"},
		{Name: "Executor", Package: "./internal/executor", Regex: "BenchmarkHandleActivate"},
		{Name: "Transport", Package: "./internal/transport", Regex: "BenchmarkHandleSendSMS"},
	}

	results := make([]benchmarkResult, 0)
	for _, suite := range suites {
		suiteResults, err := runSuite(suite, *benchtime, *count)
		if err != nil {
			fmt.Fprintf(os.Stderr, "benchmark suite %q failed: %v\n", suite.Name, err)
			os.Exit(1)
		}
		results = append(results, suiteResults...)
	}

	report := renderMarkdown(results, *benchtime, *count)
	if *output != "" {
		if err := os.WriteFile(*output, []byte(report), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write report: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Print(report)
}

func runSuite(suite benchmarkSuite, benchtime string, count int) ([]benchmarkResult, error) {
	cmd := exec.Command("go", "test", suite.Package, "-run", "^$", "-bench", suite.Regex, "-benchmem", "-benchtime", benchtime, "-count", fmt.Sprintf("%d", count))
	cmd.Env = benchmarkEnv()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}

	results := parseBenchmarkOutput(suite, stdout.Bytes())
	if len(results) == 0 {
		return nil, fmt.Errorf("no benchmark results parsed from suite output")
	}
	return results, nil
}

func benchmarkEnv() []string {
	env := os.Environ()
	if os.Getenv("GOCACHE") == "" {
		env = append(env, "GOCACHE=/tmp/go-build")
	}
	return env
}

func parseBenchmarkOutput(suite benchmarkSuite, output []byte) []benchmarkResult {
	results := make([]benchmarkResult, 0)
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		match := benchmarkLine.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		result := benchmarkResult{
			Suite:       suite.Name,
			Package:     suite.Package,
			Benchmark:   match[1],
			Iterations:  match[2],
			NsPerOp:     match[3],
			BPerOp:      match[4],
			AllocsPerOp: match[5],
		}
		results = append(results, result)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Suite == results[j].Suite {
			return results[i].Benchmark < results[j].Benchmark
		}
		return results[i].Suite < results[j].Suite
	})
	return results
}

func renderMarkdown(results []benchmarkResult, benchtime string, count int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Benchmark Report\n\n")
	fmt.Fprintf(&b, "Generated: %s UTC\n\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "- `benchtime`: `%s`\n", benchtime)
	fmt.Fprintf(&b, "- `count`: `%d`\n\n", count)
	fmt.Fprintf(&b, "| Suite | Benchmark | Iterations | ns/op | B/op | allocs/op |\n")
	fmt.Fprintf(&b, "| --- | --- | ---: | ---: | ---: | ---: |\n")
	for _, result := range results {
		bPerOp := result.BPerOp
		if bPerOp == "" {
			bPerOp = "-"
		}
		allocs := result.AllocsPerOp
		if allocs == "" {
			allocs = "-"
		}
		fmt.Fprintf(&b, "| %s | `%s` | %s | %s | %s | %s |\n", result.Suite, result.Benchmark, result.Iterations, result.NsPerOp, bPerOp, allocs)
	}
	return b.String()
}
