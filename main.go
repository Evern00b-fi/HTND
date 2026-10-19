// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	httpprof "net/http/pprof"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/HoosatNetwork/HTND/v2/app"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/autoupdate"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/config"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/rlimit"
	"github.com/HoosatNetwork/HTND/v2/version"
	"github.com/shirou/gopsutil/v3/mem"
)

func earlyInfof(format string, args ...any) {
	fmt.Fprintf(
		os.Stdout,
		"%s [INF] HSATD: %s\n",
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf(format, args...),
	)
}

// applyDefaultMemoryLimit sets defaultMemoryLimit unless GOMEMLIMIT is set. The Go runtime has already applied a set
// GOMEMLIMIT, in every form it accepts ("4GiB", "off", plain bytes); this used to re-parse it as a plain integer and
// replace anything else with the default, so GOMEMLIMIT=4GiB on a small machine ran with an 8 GB limit. The runtime
// treats an empty GOMEMLIMIT as unset, we will make some more sensible defaults if the users does not specify
const defaultMemoryLimit = 8_000_000_000 // Needed for tests

func applyDefaultMemoryLimit(lookupEnv func(string) (string, bool), setMemoryLimit func(int64) int64) {
	const GiB = 1024 * 1024 * 1024

	var targetLimit int64
	var targetGOGC int

	gogc, hasGOGC := lookupEnv("GOGC")

	if value, isSet := lookupEnv("GOMEMLIMIT"); !isSet && value != "" {
		v, err := mem.VirtualMemory()
		if err != nil {
			// Guess....
			targetLimit = 8 * GiB
			targetGOGC = 100
			debug.SetMemoryLimit(targetLimit)
			if !(hasGOGC && gogc != "") {
				debug.SetGCPercent(targetGOGC)
			}

			earlyInfof("UNABLE TO DETERMINE SYSTEM MEMORY, USING DEFAULT MEMORY LIMITS")
			earlyInfof(" -> GOMEMLIMIT set to: %d MB ", targetLimit/(1024*1024))
			if !(hasGOGC && gogc != "") {
				earlyInfof(" -> GOGC set to: %d%%", targetGOGC)
			}
			return
		}

		availableMemory := v.Available
		switch {
		case availableMemory < 4*GiB:
			// If less than 4GiB, claim 90% and set GOGC to 50
			targetLimit = int64(float64(availableMemory) * 0.90)
			targetGOGC = 50

		case availableMemory >= 4*GiB && availableMemory < 8*GiB:
			targetLimit = 8 * GiB
			if targetLimit > int64(float64(availableMemory)*0.85) {
				targetLimit = int64(float64(availableMemory) * 0.85)
			}
			targetGOGC = 80

		case availableMemory >= 8*GiB && availableMemory < 16*GiB:
			targetLimit = 8 * GiB
			targetGOGC = 100

		case availableMemory >= 16*GiB && availableMemory < 32*GiB:
			targetLimit = 16 * GiB
			if targetLimit > int64(float64(availableMemory)*0.85) {
				targetLimit = int64(float64(availableMemory) * 0.85)
			}
			targetGOGC = 100

		default: // 32GiB or more
			// If 32GiB or more available, set limit to 16GiB and GOGC to 100
			targetLimit = 16 * GiB
			targetGOGC = 100
		}

		debug.SetMemoryLimit(targetLimit)
		debug.SetGCPercent(targetGOGC)

		earlyInfof("Automated golang memory management based on system:")
		earlyInfof(" -> GOMEMLIMIT set to: %d MB (System Available: %d MB of %d MB)", targetLimit/(1024*1024), availableMemory/(1024*1024), v.Total/(1024*1024))
		if !(hasGOGC && gogc != "") {
			earlyInfof(" -> GOGC set to: %d%%", targetGOGC)
		}
	}
}

func getEnvStr(key string, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func init() {
	if v := getEnvStr("GOGC", "100"); v != "" {
		if pct, err := strconv.Atoi(v); err == nil {
			debug.SetGCPercent(pct)
		}
	}
	applyDefaultMemoryLimit(os.LookupEnv, debug.SetMemoryLimit)
	runtime.GOMAXPROCS(runtime.NumCPU())
}

// reportPanicToGitHub creates a GitHub issue for panics using shared GitHubClient
func reportPanicToGitHub(githubClient *autoupdate.GitHubClient, panicMsg any, stack []byte, autoReport bool) {
	if !autoReport {
		return
	}
	go func() {
		title := fmt.Sprintf("[PANIC] HTND v%s", version.Version())
		body := fmt.Sprintf(`**Node Crashed with Panic**

**Node Information:**
- Version: %s
- OS: %s
- Architecture: %s
- Timestamp: %s

**Panic Message:**
%v

**Stack Trace:**
%s
`,
			version.Version(), runtime.GOOS, runtime.GOARCH, time.Now().UTC().Format(time.RFC3339),
			panicMsg, string(stack))

		// Use shared GitHubClient to create issue
		_, err := githubClient.CreateIssue(context.Background(), title, body, []string{"bug", "panic", "auto-reported"})
		if err != nil {
			log.Printf("Failed to report panic to GitHub: %v", err)
		}
	}()
}

func main() {
	// Load configuration to access autoupdate settings
	cfg, err := config.LoadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	rlimit.RaiseFileLimit()

	// Get autoupdate config from loaded config
	autoUpdateCfg := autoupdate.DefaultConfig()
	autoUpdateCfg.AutoReportIssues = bool(cfg.AutoReportIssues)

	// Initialize GitHub client for error reporting
	githubClient := autoupdate.NewGitHubClient("HoosatNetwork", "HTND")
	if autoUpdateCfg.GitHubToken != "" {
		githubClient.SetToken(autoUpdateCfg.GitHubToken)
	}

	// Recover from panics and report to GitHub
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			reportPanicToGitHub(githubClient, r, stack, autoUpdateCfg.AutoReportIssues)
			// Re-panic to maintain normal crash behavior
			panic(r)
		}
	}()

	// HTND_PROFILER turns on block and mutex profiling. It also serves pprof on 127.0.0.1:6060, unless
	// --profile names the address, in which case the server --profile starts serves these profiles
	// and a second node on the same machine is not left without a profiler by the fixed port.
	if os.Getenv("HTND_PROFILER") != "" {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
	}
	if os.Getenv("HTND_PROFILER") != "" && cfg.Profile == "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/", http.RedirectHandler("/debug/pprof", http.StatusSeeOther))
			mux.HandleFunc("/debug/pprof/", httpprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", httpprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", httpprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", httpprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", httpprof.Trace)

			srv := &http.Server{
				Addr:         "127.0.0.1:6060",
				Handler:      mux,
				ReadTimeout:  5 * time.Second,
				WriteTimeout: 10 * time.Second,
				IdleTimeout:  120 * time.Second,
			}
			log.Println(srv.ListenAndServe())
		}()
	}

	if err := app.StartAppWithConfig(cfg); err != nil {
		os.Exit(1)
	}
}
