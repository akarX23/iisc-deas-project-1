import gradio as gr
import pandas as pd
import os
from typing import Tuple
import glob
import time
import json
import subprocess
import tempfile


CONFIG_FILE = "benchmark_configs.json"
TEMP_CONFIG_FILE = "/tmp/ui_benchmark_config.json"

# Basic configuration template
DEFAULT_CONFIG = [
    {
        "name": "BASE",
        "num_workers": 1,
        "mem_per_worker": 120,
        "cores_per_worker": 4,
        "dataset_scale": 1.0,
        "log_dir": "./logs/project-test",
        "remark": "Baseline configuration with 1 worker"
    }
]

def load_saved_configurations() -> str:
    """Load configurations from the saved file."""
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                configs = json.load(f)
            return json.dumps(configs, indent=2)
        else:
            # Return default configuration if file doesn't exist
            return json.dumps(DEFAULT_CONFIG, indent=2)
    except Exception as e:
        return json.dumps([{"error": f"Error loading configurations: {str(e)}"}], indent=2)


def validate_and_save_config(config_json: str) -> Tuple[str, str]:
    """
    Validate the JSON configuration and save it.
    
    Args:
        config_json: JSON string of configurations
    
    Returns:
        Tuple of (status message, validated JSON)
    """
    try:
        # Parse JSON to validate
        configs = json.loads(config_json)
        
        # Validate it's a list
        if not isinstance(configs, list):
            return "❌ Configuration must be a JSON array (list)", config_json
        
        # Validate each configuration has required fields
        required_fields = ["name", "num_workers", "mem_per_worker", "cores_per_worker", "dataset_scale", "log_dir"]
        for i, config in enumerate(configs):
            for field in required_fields:
                if field not in config:
                    return f"❌ Configuration {i} is missing required field: {field}", config_json
        
        # Save to temp file for running benchmarks
        with open(TEMP_CONFIG_FILE, 'w') as f:
            json.dump(configs, f, indent=2)
        
        return f"✅ Configuration validated! {len(configs)} configuration(s) ready to run.", json.dumps(configs, indent=2)
    
    except json.JSONDecodeError as e:
        return f"❌ Invalid JSON: {str(e)}", config_json
    except Exception as e:
        return f"❌ Error: {str(e)}", config_json


def run_benchmarks_script(config_json: str, progress=gr.Progress()) -> Tuple[str, str, str]:
    """
    Run the benchmarks script with the provided configuration.
    
    Args:
        config_json: JSON string of configurations
        progress: Gradio progress tracker
    
    Returns:
        Tuple of (status message, log output, results dataframe as markdown)
    """
    try:
        # Validate and save config to temp file
        try:
            configs = json.loads(config_json)
            if not isinstance(configs, list):
                return "❌ Configuration must be a JSON array", "", ""
            
            with open(TEMP_CONFIG_FILE, 'w') as f:
                json.dump(configs, f, indent=2)
        except json.JSONDecodeError as e:
            return f"❌ Invalid JSON: {str(e)}", "", ""
        
        num_configs = len(configs)
        
        if num_configs == 0:
            return "❌ No configurations to run!", "", ""
        
        progress(0, desc=f"🚀 Starting benchmark script for {num_configs} configurations...")
        
        # Start the benchmark script in a subprocess
        process = subprocess.Popen(
            ['bash', 'run_benchmarks.sh', TEMP_CONFIG_FILE],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        log_output = []
        
        progress(0.5, desc=f"⏳ Running {num_configs} benchmark configuration(s)... Please wait...")
        
        # Read output in real-time
        for line in iter(process.stdout.readline, ''):
            if line:
                log_output.append(line.rstrip())
        
        process.wait()
        
        if process.returncode == 0:
            progress(1.0, desc="✅ All benchmarks completed!")
            
            # Load and display results
            results_md = load_all_results("./logs/*")
            
            status = f"""
✅ **All Benchmarks Completed Successfully!**

- Total Configurations: {num_configs}
- Results saved in CSV files

Check the results below or in the "All Results" tab.
"""
            return status, "\n".join(log_output), results_md
        else:
            return (
                f"❌ **Benchmark Script Failed**\n\nReturn code: {process.returncode}",
                "\n".join(log_output),
                ""
            )
    
    except Exception as e:
        return f"❌ **Error Running Benchmarks**\n\n{str(e)}", "", ""


def load_results_from_file(file_path: str) -> str:
    """
    Load results from CSV file and return as formatted markdown table.
    
    Args:
        file_path: Path to the results CSV file
    
    Returns:
        Formatted markdown table
    """
    try:
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            return df.to_markdown(index=False)
        else:
            return "Results file not found."
    except Exception as e:
        return f"Error loading results: {str(e)}"


def load_all_results(log_dir_pattern: str = "./logs/*") -> str:
    """
    Load all results from log directories matching the pattern.
    
    Args:
        log_dir_pattern: Glob pattern for log directories
    
    Returns:
        Combined results as markdown table
    """
    try:
        # Find all results.csv files
        results_files = glob.glob(f"{log_dir_pattern}/results.csv")
        
        if not results_files:
            return "No results found. Run a benchmark first!"
        
        # Load and combine all results
        all_dfs = []
        for file_path in results_files:
            try:
                df = pd.read_csv(file_path)
                df['log_dir'] = os.path.dirname(file_path)
                all_dfs.append(df)
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
        
        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            # Sort by E2E_time
            combined_df = combined_df.sort_values('E2E_time')
            return combined_df.to_markdown(index=False)
        else:
            return "No valid results found."
    
    except Exception as e:
        return f"Error loading results: {str(e)}"


# Create Gradio interface
with gr.Blocks(title="Spark Pipeline Benchmark", theme=gr.themes.Soft()) as demo:
    gr.Markdown(
        """
        # 🚀 Spark Pipeline Benchmark Tool
        
        Edit your benchmark configurations and execute them sequentially.
        Docker containers will be automatically managed for each configuration.
        """
    )
    
    gr.Markdown("---")
    
    # Configuration Editor Section
    gr.Markdown("## ⚙️ Benchmark Configurations")
    gr.Markdown("Edit the JSON configuration below. Each object in the array represents one benchmark configuration.")
    
    with gr.Row():
        with gr.Column():
            configurations_json = gr.Code(
                label="Configurations (JSON)",
                language="json",
                value=load_saved_configurations(),
                lines=25,
                interactive=True
            )
            
            with gr.Row():
                validate_btn = gr.Button("✅ Validate Configuration", size="sm")
                load_btn = gr.Button("� Reload from File", size="sm")
            
            config_status = gr.Textbox(label="Validation Status", interactive=False, lines=2)
    
    # Run Benchmarks Section
    gr.Markdown("---")
    gr.Markdown("## ▶️ Execute Benchmarks")
    gr.Markdown("Run all configured benchmarks sequentially. Docker containers will be created and destroyed for each configuration.")
    
    run_benchmarks_btn = gr.Button("🚀 Run All Benchmarks", variant="primary", size="lg")
    
    with gr.Row():
        benchmark_status = gr.Textbox(
            label="Execution Status",
            value="Ready to run benchmarks",
            interactive=False,
            lines=3
        )
    
    # Results Section
    gr.Markdown("---")
    gr.Markdown("## 📊 Benchmark Results")
    
    with gr.Tabs():
        with gr.Tab("Execution Log"):
            gr.Markdown("Real-time output from the benchmark script")
            log_output = gr.Textbox(
                label="Script Output",
                lines=25,
                max_lines=50,
                interactive=False,
                show_copy_button=True
            )
        
        with gr.Tab("Latest Results"):
            latest_results = gr.Markdown(label="Results from Latest Run")
        
        with gr.Tab("All Results"):
            gr.Markdown("View all benchmark results from log directories")
            log_pattern = gr.Textbox(
                value="./logs/*",
                label="Log Directory Pattern",
                info="Glob pattern to search for results"
            )
            load_all_btn = gr.Button("📂 Load All Results", size="sm")
            all_results_output = gr.Markdown(label="All Results")
    
    # Connect the buttons
    validate_btn.click(
        fn=validate_and_save_config,
        inputs=[configurations_json],
        outputs=[config_status, configurations_json]
    )
    
    load_btn.click(
        fn=load_saved_configurations,
        outputs=[configurations_json]
    )
    
    run_benchmarks_btn.click(
        fn=lambda: "🔄 Running benchmarks...",
        outputs=[benchmark_status]
    ).then(
        fn=run_benchmarks_script,
        inputs=[configurations_json],
        outputs=[benchmark_status, log_output, latest_results]
    )
    
    load_all_btn.click(
        fn=load_all_results,
        inputs=[log_pattern],
        outputs=[all_results_output]
    )
    
    # Footer
    gr.Markdown(
        """
        ---
        **Instructions:**
        1. Edit the JSON configuration above (or load from file)
        2. Click "Validate Configuration" to check if the JSON is valid
        3. Click "Run All Benchmarks" to execute them sequentially
        4. The script will automatically create and destroy Docker containers for each configuration
        5. Monitor progress in the Execution Log tab
        6. View results in the Latest Results or All Results tabs
        
        **Configuration Format:**
        ```json
        [
          {
            "name": "BASE",
            "num_workers": 1,
            "mem_per_worker": 120,
            "cores_per_worker": 4,
            "dataset_scale": 1.0,
            "log_dir": "./logs/project-test",
            "remark": "Description of this configuration"
          }
        ]
        ```
        
        **Note:** Make sure Docker is running and you have the necessary permissions.
        """
    )


if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860, share=False)
