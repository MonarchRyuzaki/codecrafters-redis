#!/usr/bin/env python3
import os
import re
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

# Define command groups based on bench.sh
COMMAND_GROUPS = {
    "Standard Commands": ["PING_INLINE", "PING_BULK", "SET", "GET", "RPUSH", "LPOP"],
    "Slow List Commands": ["LPUSH", "LRANGE"],
    "Additional Commands": ["ECHO", "LLEN", "TYPE", "BLPOP"],
    "Stream Commands": ["XADD", "XRANGE", "XREAD"]
}

def parse_benchmark_log(log_path):
    """Parses intermediate/final throughput and full latency distribution."""
    with open(log_path, 'r') as f:
        lines = f.readlines()

    results = []
    current_intermediate = []
    
    inter_regex = re.compile(r"^(.+?):\s+([\d.]+)$")
    header_regex = re.compile(r"^====== (.*?) ======$")
    final_rps_regex = re.compile(r"([\d.]+)\s+requests per second")
    latency_regex = re.compile(r"([\d.]+)%\s+<=\s+([\d.]+)\s+milliseconds")

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        inter_match = inter_regex.match(line)
        if inter_match and not any(k in line for k in ["keep alive", "parallel clients", "bytes payload"]):
            try:
                current_intermediate.append(float(inter_match.group(2)))
            except ValueError: pass
            i += 1
            continue
            
        header_match = header_regex.match(line)
        if header_match:
            header = header_match.group(1).strip()
            op_name_display = header
            op_name_match = header.split()[0].upper()
            
            block_content = ""
            i += 1
            while i < len(lines):
                next_line = lines[i].strip()
                if header_regex.match(next_line) or (inter_regex.match(next_line) and not any(k in next_line for k in ["keep alive", "parallel clients", "bytes payload"])):
                    break
                block_content += next_line + "\n"
                i += 1
            
            final_match = final_rps_regex.search(block_content)
            final_rps = float(final_match.group(1)) if final_match else None
            
            # Extract full distribution
            lat_matches = latency_regex.findall(block_content)
            full_dist = [(float(pct), float(ms)) for pct, ms in lat_matches]
            
            rps_series = list(current_intermediate)
            if final_rps: rps_series.append(final_rps)
            
            results.append({
                "Operation": op_name_display,
                "OpMatch": op_name_match,
                "RPS": final_rps,
                "RPS_Series": rps_series,
                "Full_Dist": full_dist
            })
            current_intermediate = []
            continue
        i += 1
    
    return pd.DataFrame(results)

def parse_usage_csv(csv_path):
    """Parses CPU and Memory usage from CSV."""
    df = pd.read_csv(csv_path)
    df['Memory(MB)'] = df['Memory(KB)'] / 1024.0
    df['Time'] = pd.to_datetime(df['Time'].str.strip(), format='%H:%M:%S')
    return df

def plot_throughput(df, output_prefix):
    """Creates line charts for RPS evolution."""
    for group_name, ops in COMMAND_GROUPS.items():
        group_df = df[df['OpMatch'].isin([o.upper() for o in ops])]
        if group_df.empty: continue
            
        plt.figure(figsize=(12, 6))
        for _, row in group_df.iterrows():
            series = row['RPS_Series']
            if len(series) > 1:
                plt.plot(range(len(series)), series, marker='o', linewidth=2, label=row['Operation'])
            elif series:
                plt.scatter([0], series, label=row['Operation'])
        
        plt.title(f'Throughput Evolution: {group_name}', fontsize=14)
        plt.ylabel('RPS')
        plt.xlabel('Sample Index')
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.savefig(f"{output_prefix}throughput_{group_name.lower().replace(' ', '_')}.png")
        plt.close()

def plot_latency_distribution(df, output_prefix):
    """Creates Violin Plots and CDF plots for latency."""
    for group_name, ops in COMMAND_GROUPS.items():
        group_df = df[df['OpMatch'].isin([o.upper() for o in ops])]
        if group_df.empty: continue

        # 1. Violin Plot (Density Shape)
        plt.figure(figsize=(12, 8))
        all_samples = []
        labels = []
        for _, row in group_df.iterrows():
            dist = row['Full_Dist']
            if not dist: continue
            
            pcts, mss = zip(*dist)
            sample_pcts = np.linspace(0, 100, 1000)
            sample_mss = np.interp(sample_pcts, pcts, mss)
            all_samples.append(sample_mss)
            labels.append(row['Operation'])
            
        if all_samples:
            parts = plt.violinplot(all_samples, showmedians=True)
            for pc in parts['bodies']:
                pc.set_facecolor('#D43F33')
                pc.set_edgecolor('black')
                pc.set_alpha(0.7)
            
            plt.xticks(range(1, len(labels) + 1), labels, rotation=45, ha='right')
            plt.ylabel('Latency (ms)')
            plt.title(f'Latency Distribution Shape: {group_name}')
            plt.grid(True, axis='y', linestyle='--', alpha=0.5)
            
            if any(max(s) > 20 for s in all_samples):
                plt.yscale('log')
                plt.ylabel('Latency (ms) [Log Scale]')
                
            plt.tight_layout()
            plt.savefig(f"{output_prefix}latency_violin_{group_name.lower().replace(' ', '_')}.png")
        plt.close()

        # 2. Percentile CDF Plot (Tail Analysis)
        plt.figure(figsize=(12, 6))
        for _, row in group_df.iterrows():
            dist = row['Full_Dist']
            if not dist: continue
            pcts, mss = zip(*dist)
            plt.plot(mss, pcts, marker='.', label=row['Operation'])
        
        plt.title(f'Latency CDF: {group_name}', fontsize=14)
        plt.xlabel('Latency (ms)')
        plt.ylabel('Percentile (%)')
        plt.xscale('log')
        plt.grid(True, linestyle='--', alpha=0.6)
        
        # Only draw legend if data was plotted
        if plt.gca().get_legend_handles_labels()[0]:
            plt.legend(loc='lower right')
            
        plt.tight_layout()
        plt.savefig(f"{output_prefix}latency_cdf_{group_name.lower().replace(' ', '_')}.png")
        plt.close()

def plot_resources(usage_df, output_prefix):
    """Plots CPU and Memory utilization."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
    
    ax1.plot(usage_df['Time'], usage_df['CPU'], color='tab:blue', linewidth=2)
    ax1.set_title('CPU Utilization', fontsize=12)
    ax1.set_ylabel('CPU %')
    ax1.grid(True, alpha=0.3)
    
    ax2.plot(usage_df['Time'], usage_df['Memory(MB)'], color='tab:green', linewidth=2)
    ax2.set_title('Memory Utilization', fontsize=12)
    ax2.set_ylabel('Memory (MB)')
    ax2.set_xlabel('Time')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f"{output_prefix}resources.png")
    plt.close()

def main():
    parser = argparse.ArgumentParser(description='Redis Benchmark Visualization')
    parser.add_argument('--benchmark-log', required=True)
    parser.add_argument('--usage-csv', required=True)
    args = parser.parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = f"benchmark_results/graphs/bench_{ts}"
    os.makedirs(prefix, exist_ok=True)
    prefix += "/"

    print(f"Generating rich visualizations...")
    
    try:
        results_df = parse_benchmark_log(args.benchmark_log)
        usage_df = parse_usage_csv(args.usage_csv)
        
        plot_throughput(results_df, prefix)
        plot_latency_distribution(results_df, prefix)
        plot_resources(usage_df, prefix)
        
        print(f"Success! Graphs saved with prefix: {prefix}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback; traceback.print_exc()

if __name__ == "__main__":
    main()
