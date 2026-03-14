package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"time"
)

type apiClient struct {
	baseURL string
	http    *http.Client
}

type createProfileResponse struct {
	Data struct {
		ID           string `json:"id"`
		Name         string `json:"name"`
		Applications []struct {
			ID string `json:"id"`
		} `json:"applications"`
	} `json:"data"`
}

type datasetManifest struct {
	Prefix           string            `json:"prefix"`
	Profiles         int               `json:"profiles"`
	CardsPerProfile  int               `json:"cards_per_profile"`
	GeneratedAt      time.Time         `json:"generated_at"`
	ProfileManifests []profileManifest `json:"profile_manifests"`
}

type profileManifest struct {
	Index         int    `json:"index"`
	ProfileName   string `json:"profile_name"`
	ProfileID     string `json:"profile_id"`
	ApplicationID string `json:"application_id"`
	CardCount     int    `json:"card_count"`
	FirstICCID    string `json:"first_iccid"`
	FirstMSISDN   string `json:"first_msisdn"`
}

func main() {
	baseURL := flag.String("base-url", "http://127.0.0.1:8080", "OTA API base URL")
	prefix := flag.String("prefix", "seed", "profile/card prefix")
	profiles := flag.Int("profiles", 10, "number of profiles")
	cardsPerProfile := flag.Int("cards-per-profile", 10000, "cards per profile")
	timeout := flag.Duration("timeout", 30*time.Minute, "HTTP client timeout")
	outPath := flag.String("out", "/tmp/ota-seed-manifest.json", "path to write manifest JSON")
	flag.Parse()

	client := &apiClient{
		baseURL: strings.TrimRight(*baseURL, "/"),
		http:    &http.Client{Timeout: *timeout},
	}

	manifest := datasetManifest{
		Prefix:          *prefix,
		Profiles:        *profiles,
		CardsPerProfile: *cardsPerProfile,
		GeneratedAt:     time.Now().UTC(),
	}

	for i := 0; i < *profiles; i++ {
		profileName := fmt.Sprintf("%s-profile-%02d", *prefix, i+1)
		profileID, appID, err := createProfile(client, profileName)
		if err != nil {
			fatalf("create profile %s: %v", profileName, err)
		}
		if err := importProfileCards(client, *prefix, profileName, i, *cardsPerProfile); err != nil {
			fatalf("import cards for %s: %v", profileName, err)
		}
		firstICCID, firstMSISDN := cardIdentifiers(*prefix, i, 0)
		manifest.ProfileManifests = append(manifest.ProfileManifests, profileManifest{
			Index:         i + 1,
			ProfileName:   profileName,
			ProfileID:     profileID,
			ApplicationID: appID,
			CardCount:     *cardsPerProfile,
			FirstICCID:    firstICCID,
			FirstMSISDN:   firstMSISDN,
		})
		fmt.Printf("loaded %d cards into %s (%s)\n", *cardsPerProfile, profileName, profileID)
	}

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(*outPath, data, 0o644); err != nil {
		fatalf("write manifest: %v", err)
	}
	fmt.Printf("manifest written to %s\n", *outPath)
}

func createProfile(client *apiClient, profileName string) (string, string, error) {
	body := map[string]any{
		"name":           profileName,
		"max_concat_sms": 5,
		"buffer_size":    140,
		"applications": []map[string]any{{
			"name":               profileName + "-app",
			"tar":                "010203",
			"kic_algo":           "DES",
			"kic_mode":           "TRIPLE_DES_CBC_2_KEYS",
			"kic_keyset_id":      1,
			"kid_algo":           "DES",
			"kid_mode":           "TRIPLE_DES_CBC_2_KEYS",
			"kid_keyset_id":      1,
			"certification_mode": "CC",
			"ciphered":           true,
			"counter_mode":       "COUNTER_REPLAY_OR_CHECK",
			"por_mode":           "REPLY_ALWAYS",
			"por_protocol":       "SMS_SUBMIT",
			"por_ciphered":       false,
			"por_cert_mode":      "NO_SECURITY",
		}},
	}

	var resp createProfileResponse
	if err := postJSON(client, "/api/v1/profiles", body, http.StatusCreated, &resp); err != nil {
		return "", "", err
	}
	if resp.Data.ID == "" || len(resp.Data.Applications) != 1 || resp.Data.Applications[0].ID == "" {
		return "", "", fmt.Errorf("unexpected profile response")
	}
	return resp.Data.ID, resp.Data.Applications[0].ID, nil
}

func importProfileCards(client *apiClient, prefix, profileName string, profileIndex, count int) error {
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)

	go func() {
		defer pw.Close()
		defer mw.Close()

		fw, err := mw.CreateFormFile("file", profileName+".csv")
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}

		w := csv.NewWriter(fw)
		if err := w.Write([]string{"iccid", "imsi", "msisdn", "profile_name", "enc_key", "auth_key"}); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		for i := 0; i < count; i++ {
			iccid, msisdn := cardIdentifiers(prefix, profileIndex, i)
			imsi := fmt.Sprintf("%s-imsi-%02d-%05d", prefix, profileIndex+1, i)
			if err := w.Write([]string{
				iccid,
				imsi,
				msisdn,
				profileName,
				"404142434445464748494A4B4C4D4E4F",
				"505152535455565758595A5B5C5D5E5F",
			}); err != nil {
				_ = pw.CloseWithError(err)
				return
			}
		}
		w.Flush()
		if err := w.Error(); err != nil {
			_ = pw.CloseWithError(err)
		}
	}()

	req, err := http.NewRequest(http.MethodPost, client.baseURL+"/api/v1/cards/import", pr)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := client.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("import status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func cardIdentifiers(prefix string, profileIndex, idx int) (string, string) {
	n := profileIndex*100000 + idx
	iccid := fmt.Sprintf("%s-iccid-%02d-%05d", prefix, profileIndex+1, idx)
	msisdn := fmt.Sprintf("4477%08d", n)
	return iccid, msisdn
}

func postJSON(client *apiClient, path string, body any, expected int, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, client.baseURL+path, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != expected {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s returned %d: %s", path, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
