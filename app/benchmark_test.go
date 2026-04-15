package main

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// DBInterface defines the core operations we want to benchmark.
// This interface makes it easy to swap implementations in the future.
type DBInterface interface {
	Set(key string, value MapValue)
	Get(key string) (MapValue, bool)
	
	RPUSH(key string, items []string) (int, error)
	LPUSH(key string, items []string) (int, error)
	LPOP(key string, cnt int) ([]string, error)
	LRANGE(key string, start int, end int) []string

	XADD(key string, id string, streamValue map[string]string) (string, error)
	XRANGE(key, start, end string) (StreamValue, error)
	XReadStream(key, lastID string, timeout int) (StreamValue, error)
}

type BenchmarkResult struct {
	Name        string
	OpsPerSec   float64
	P50         time.Duration
	P95         time.Duration
	P99         time.Duration
	MaxLatency  time.Duration
	CPUUsagePct float64
	MemAllocMB  float64
	MemSysMB    float64
}

type OpGroup string

const (
	GroupString OpGroup = "String"
	GroupList   OpGroup = "List"
	GroupStream OpGroup = "Stream"
)

type OpType int

const (
	OpWrite OpType = iota
	OpRead
	OpMixed
)

func TestCustomBenchmark(t *testing.T) {
	db := NewDB()
	
	results := []BenchmarkResult{}

	fmt.Println("Starting Benchmarks...")

	// Warmup
	runBenchmark("Warmup", db, GroupString, 10000, 10, OpWrite)

	// String Benchmarks
	results = append(results, runBenchmark("String Write 100k (100 workers)", db, GroupString, 100000, 100, OpWrite))
	results = append(results, runBenchmark("String Read 100k (100 workers)", db, GroupString, 100000, 100, OpRead))
	results = append(results, runBenchmark("String Mixed R/W 100k (100 workers)", db, GroupString, 100000, 100, OpMixed))

	// List Benchmarks
	results = append(results, runBenchmark("List Push 100k (100 workers)", db, GroupList, 100000, 100, OpWrite))
	results = append(results, runBenchmark("List Pop 100k (100 workers)", db, GroupList, 100000, 100, OpRead))
	results = append(results, runBenchmark("List Mixed Push/Pop 100k (100 workers)", db, GroupList, 100000, 100, OpMixed))

	// Stream Benchmarks
	results = append(results, runBenchmark("Stream Add 100k (100 workers)", db, GroupStream, 100000, 100, OpWrite))
	results = append(results, runBenchmark("Stream Read 100k (100 workers)", db, GroupStream, 100000, 100, OpRead))
	results = append(results, runBenchmark("Stream Mixed Add/Read 100k (100 workers)", db, GroupStream, 100000, 100, OpMixed))

	report := formatReport(results)
	fmt.Println("\n" + report)
	
	err := os.WriteFile("benchmark_results.txt", []byte(report), 0644)
	if err != nil {
		t.Fatalf("Failed to write report: %v", err)
	}
	fmt.Println("Results saved to benchmark_results.txt")
}

func getCPUTime() time.Duration {
	var rusage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusage)
	return time.Duration(rusage.Utime.Sec)*time.Second + time.Duration(rusage.Utime.Usec)*time.Microsecond +
		time.Duration(rusage.Stime.Sec)*time.Second + time.Duration(rusage.Stime.Usec)*time.Microsecond
}

func runBenchmark(name string, db DBInterface, group OpGroup, numOps int, concurrency int, opType OpType) BenchmarkResult {
	// Force GC to start from a clean state
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	cpuBefore := getCPUTime()

	opsPerWorker := numOps / concurrency
	latencies := make([]time.Duration, numOps)
	
	var wg sync.WaitGroup
	wg.Add(concurrency)

	// Pre-seed read keys to ensure hits if opType is OpRead or OpMixed
	if opType == OpRead || opType == OpMixed {
		for w := 0; w < concurrency; w++ {
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("bench_%s_key_%d_%d", group, w, i)
				switch group {
				case GroupString:
					db.Set(key, MapValue{Type: STRING_, Value: StringValue{Value: "test", IsPermanent: true}})
				case GroupList:
					db.RPUSH(key, []string{"test_item"})
				case GroupStream:
					db.XADD(key, "*", map[string]string{"test_field": "test_value"})
				}
			}
		}
	}

	startTime := time.Now()

	for w := 0; w < concurrency; w++ {
		go func(workerID int) {
			defer wg.Done()
			
			startIdx := workerID * opsPerWorker
			
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("bench_%s_key_%d_%d", group, workerID, i)
				
				opStart := time.Now()
				
				switch group {
				case GroupString:
					val := MapValue{Type: STRING_, Value: StringValue{Value: "test", IsPermanent: true}}
					switch opType {
					case OpWrite: db.Set(key, val)
					case OpRead: db.Get(key)
					case OpMixed:
						if i%2 == 0 { db.Set(key, val) } else { db.Get(key) }
					}
				case GroupList:
					switch opType {
					case OpWrite: db.RPUSH(key, []string{"item"})
					case OpRead: db.LPOP(key, 1)
					case OpMixed:
						if i%2 == 0 { db.RPUSH(key, []string{"item"}) } else { db.LPOP(key, 1) }
					}
				case GroupStream:
					switch opType {
					case OpWrite: db.XADD(key, "*", map[string]string{"f1": "v1"})
					case OpRead: db.XRANGE(key, "-", "+")
					case OpMixed:
						if i%2 == 0 {
							db.XADD(key, "*", map[string]string{"f1": "v1"})
						} else {
							db.XRANGE(key, "-", "+")
						}
					}
				}
				
				latencies[startIdx+i] = time.Since(opStart)
			}
		}(w)
	}

	wg.Wait()
	totalDuration := time.Since(startTime)
	cpuAfter := getCPUTime()

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	p50 := latencies[int(float64(numOps)*0.50)]
	p95 := latencies[int(float64(numOps)*0.95)]
	p99 := latencies[int(float64(numOps)*0.99)]
	maxLat := latencies[numOps-1]

	opsPerSec := float64(numOps) / totalDuration.Seconds()

	allocMB := float64(memAfter.Alloc-memBefore.Alloc) / 1024 / 1024
	sysMB := float64(memAfter.Sys-memBefore.Sys) / 1024 / 1024

	cpuTimeUsed := cpuAfter - cpuBefore
	cpuUsagePct := (float64(cpuTimeUsed) / float64(totalDuration)) * 100.0

	return BenchmarkResult{
		Name:        name,
		OpsPerSec:   opsPerSec,
		P50:         p50,
		P95:         p95,
		P99:         p99,
		MaxLatency:  maxLat,
		CPUUsagePct: cpuUsagePct,
		MemAllocMB:  allocMB,
		MemSysMB:    sysMB,
	}
}

func formatReport(results []BenchmarkResult) string {
	var sb strings.Builder
	sb.WriteString("====================================================================================================================================\n")
	sb.WriteString(fmt.Sprintf("%-42s | %-12s | %-10s | %-10s | %-10s | %-10s | %-15s\n", "Benchmark Name", "Ops/sec", "p50", "p95", "p99", "CPU %", "Mem Alloc (MB)"))
	sb.WriteString("====================================================================================================================================\n")
	
	for _, r := range results {
		sb.WriteString(fmt.Sprintf("%-42s | %-12.0f | %-10v | %-10v | %-10v | %-10.1f | %-15.2f\n", 
			r.Name, r.OpsPerSec, r.P50, r.P95, r.P99, r.CPUUsagePct, r.MemAllocMB))
	}
	sb.WriteString("====================================================================================================================================\n")
	return sb.String()
}
