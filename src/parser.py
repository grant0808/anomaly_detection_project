import os
import re
import pandas as pd
from tqdm import tqdm
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

def extract_block_id(log_line):
    # Regex to find block ID like blk_-1608999687919862906
    match = re.search(r'(blk_-?\d+)', log_line)
    return match.group(1) if match else None

def clean_log_message(log_line):
    # HDFS log format: Date Time Pid Level Component: Message
    # Example: 081109 203615 148 INFO dfs.DataNode$PacketResponder: PacketResponder 1 for block blk_-1608999687919862906 terminating
    # We want to extract the message after the component.
    match = re.match(r'^\d{6}\s+\d{6}\s+\d+\s+[A-Z]+\s+[\w$.]+:\s+(.*)$', log_line)
    if match:
        msg = match.group(1)
    else:
        # Fallback if format is slightly different
        parts = log_line.split(':', 5)
        if len(parts) > 1:
            msg = ':'.join(parts[1:]).strip()
        else:
            msg = log_line.strip()
            
    # Replace block IDs with placeholder to help Drain3 cluster better
    msg = re.sub(r'blk_-?\d+', '[block_id]', msg)
    # Replace IP addresses
    msg = re.sub(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', '[ip]', msg)
    # Replace numbers
    msg = re.sub(r'\b\d+\b', '[num]', msg)
    return msg

def parse_logs(log_file_path, max_lines=100000):
    print(f"Starting log parsing on {log_file_path} (max_lines={max_lines})...")
    
    # Configure Drain3 template miner
    config = TemplateMinerConfig()
    config.load(os.path.join(os.path.dirname(__file__), 'drain3.ini'))
    template_miner = TemplateMiner(config=config)
    
    block_events = {}
    
    with open(log_file_path, 'r', encoding='utf-8') as f:
        for idx, line in enumerate(tqdm(f, total=max_lines)):
            if idx >= max_lines:
                break
                
            block_id = extract_block_id(line)
            if not block_id:
                continue
                
            cleaned_msg = clean_log_message(line)
            result = template_miner.add_log_message(cleaned_msg)
            
            # Use cluster_id as the Event ID (e.g. E1, E2...)
            event_id = f"E{result['cluster_id']}"
            
            if block_id not in block_events:
                block_events[block_id] = []
            block_events[block_id].append(event_id)
            
    print(f"Parsed {len(block_events)} unique block traces.")
    print(f"Total templates found: {len(template_miner.drain.clusters)}")
    
    # Print some templates for validation
    print("\nSample Templates Found:")
    for cluster in list(template_miner.drain.clusters)[:10]:
        print(f"ID {cluster.cluster_id}: {cluster.get_template()}")
        
    return block_events, template_miner

if __name__ == '__main__':
    # Test execution
    log_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../HDFS_v1/HDFS.log'))
    drain_config_path = os.path.join(os.path.dirname(__file__), 'drain3.ini')
    
    # Create default drain3.ini if it doesn't exist
    if not os.path.exists(drain_config_path):
        ini_content = """[DRAIN]
sim_th = 0.5
depth = 4
max_children = 100
max_clusters = 1024
extra_delimiters = ["_", "-"]
"""
        with open(drain_config_path, 'w') as f:
            f.write(ini_content)
            
    block_events, miner = parse_logs(log_path, max_lines=100000)
    
    # Save the parsed traces to a CSV
    data = []
    for bid, events in block_events.items():
        data.append({
            'BlockId': bid,
            'EventSequence': ' '.join(events)
        })
    df = pd.DataFrame(data)
    out_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../preprocessed_sample.csv'))
    df.to_csv(out_path, index=False)
    print(f"Saved preprocessed sample traces to {out_path}")
