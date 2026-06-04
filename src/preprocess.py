import json
import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

class EventIndexer:
    def __init__(self):
        # Index 0 is reserved for padding <PAD>
        self.event2idx = {"<PAD>": 0}
        self.idx2event = {0: "<PAD>"}
        self.num_events = 1

    def fit(self, event_sequences):
        for seq in event_sequences:
            for event in seq:
                if event not in self.event2idx:
                    self.event2idx[event] = self.num_events
                    self.idx2event[self.num_events] = event
                    self.num_events += 1
        return self

    def transform(self, event_sequence):
        return [self.event2idx.get(event, 0) for event in event_sequence]

    def save(self, filepath):
        with open(filepath, 'w') as f:
            json.dump({
                'event2idx': self.event2idx,
                'idx2event': {str(k): v for k, v in self.idx2event.items()}
            }, f, indent=4)

    @classmethod
    def load(cls, filepath):
        indexer = cls()
        with open(filepath, 'r') as f:
            data = json.load(f)
            indexer.event2idx = data['event2idx']
            indexer.idx2event = {int(k): v for k, v in data['idx2event'].items()}
            indexer.num_events = len(indexer.event2idx)
        return indexer

def generate_sliding_windows(sequences, window_size=10):
    """
    Generates inputs and targets using a sliding window.
    For sequence [e1, e2, e3] and W=3:
    - Input: [PAD, PAD, PAD], Target: e1
    - Input: [PAD, PAD, e1], Target: e2
    - Input: [PAD, e1, e2], Target: e3
    """
    X = []
    y = []
    for seq in sequences:
        for i in range(len(seq)):
            target = seq[i]
            # Slicing window
            start_idx = max(0, i - window_size)
            input_seq = seq[start_idx:i]
            # Pad if shorter than window_size
            pad_len = window_size - len(input_seq)
            padded_input = [0] * pad_len + input_seq
            
            X.append(padded_input)
            y.append(target)
            
    return np.array(X, dtype=np.int32), np.array(y, dtype=np.int32)

def prepare_dataset(csv_path, vocab_path, window_size=10, is_preprocessed_file=True, test_size=0.2, random_state=42):
    """
    Loads traces, fits vocab indexer, splits into train/test, and creates sliding windows.
    If is_preprocessed_file=True, parses HDFS_v1/preprocessed/Event_traces.csv.
    Otherwise, parses the custom preprocessed_sample.csv.
    """
    print(f"Loading dataset from {csv_path}...")
    df = pd.read_csv(csv_path)
    
    # 1. Parse Event Sequences
    sequences = []
    labels = []
    
    if is_preprocessed_file:
        # Event_traces.csv format: Features is string "[E5, E22, ...]"
        # Label is "Success" (Normal) or "Fail" (Anomaly)
        for _, row in df.iterrows():
            features_str = row['Features']
            # Convert string representation of list to actual list
            events = [e.strip().strip("'").strip('"') for e in features_str.strip('[]').split(',') if e.strip()]
            sequences.append(events)
            labels.append(0 if row['Label'] == 'Success' else 1)
    else:
        # custom preprocessed_sample.csv format: EventSequence is space-separated string "E5 E22 E5..."
        # We need to map BlockId to Normal/Anomaly label using anomaly_label.csv if available
        # Default labels to 0 (Normal) for custom sample training
        anomaly_label_path = os.path.join(os.path.dirname(csv_path), 'preprocessed/anomaly_label.csv')
        label_map = {}
        if os.path.exists(anomaly_label_path):
            labels_df = pd.read_csv(anomaly_label_path)
            label_map = dict(zip(labels_df['BlockId'], labels_df['Label']))
            
        for _, row in df.iterrows():
            seq_str = str(row['EventSequence'])
            events = seq_str.split() if seq_str and seq_str != 'nan' else []
            sequences.append(events)
            
            bid = row['BlockId']
            label_str = label_map.get(bid, 'Normal')
            labels.append(0 if label_str == 'Normal' else 1)
            
    # 2. Fit/load indexer
    indexer = EventIndexer()
    indexer.fit(sequences)
    indexer.save(vocab_path)
    print(f"Saved event vocabulary mapping ({indexer.num_events} tokens) to {vocab_path}")
    
    # Map sequences to indices
    indexed_sequences = [indexer.transform(seq) for seq in sequences]
    
    # 3. Train/Test Split (stratified by label)
    train_seqs, test_seqs, train_labels, test_labels = train_test_split(
        indexed_sequences, labels, test_size=test_size, random_state=random_state, stratify=labels
    )
    
    # 4. Filter only Normal traces for DeepLog training
    # DeepLog trains ONLY on normal sequences!
    normal_train_seqs = [train_seqs[i] for i in range(len(train_seqs)) if train_labels[i] == 0]
    
    # 5. Generate sliding windows for training (Normal only)
    X_train, y_train = generate_sliding_windows(normal_train_seqs, window_size)
    
    # For testing, we generate sliding windows per sequence to evaluate anomaly detection rate
    # We return the test sequences and their labels directly for custom evaluation
    print(f"Dataset summary:")
    print(f"  Total traces: {len(sequences)}")
    print(f"  Training normal traces: {len(normal_train_seqs)}")
    print(f"  Training sliding windows (X_train): {X_train.shape}")
    print(f"  Testing traces: {len(test_seqs)} (Normal: {test_labels.count(0)}, Anomaly: {test_labels.count(1)})")
    
    return X_train, y_train, test_seqs, test_labels, indexer

if __name__ == '__main__':
    # Test execution
    base_dir = os.path.dirname(__file__)
    csv_path = os.path.abspath(os.path.join(base_dir, '../HDFS_v1/preprocessed/Event_traces.csv'))
    vocab_path = os.path.abspath(os.path.join(base_dir, 'vocab.json'))
    
    if os.path.exists(csv_path):
        X_tr, y_tr, test_s, test_l, idx = prepare_dataset(csv_path, vocab_path, window_size=10)
        print("Preprocessing test complete!")
    else:
        print(f"HDFS Event_traces.csv not found at {csv_path}. Please check file path.")
