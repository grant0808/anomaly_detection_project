import torch
import torch.nn as nn
import numpy as np

class DeepLogLSTM(nn.Module):
    def __init__(self, num_classes, embedding_dim=64, hidden_size=64, num_layers=2):
        super(DeepLogLSTM, self).__init__()
        self.num_classes = num_classes
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        
        # Embedding layer: maps Event ID to a dense representation
        self.embedding = nn.Embedding(num_classes, embedding_dim, padding_idx=0)
        
        # LSTM layer
        self.lstm = nn.LSTM(
            input_size=embedding_dim,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True
        )
        
        # Linear layer mapping hidden state to class logits
        self.fc = nn.Linear(hidden_size, num_classes)

    def forward(self, x):
        # x shape: (batch_size, seq_len)
        embedded = self.embedding(x)  # shape: (batch_size, seq_len, embedding_dim)
        
        # LSTM forward pass
        out, _ = self.lstm(embedded)  # out shape: (batch_size, seq_len, hidden_size)
        
        # Take the last time step output
        last_step_out = out[:, -1, :]  # shape: (batch_size, hidden_size)
        
        # Logits mapping
        logits = self.fc(last_step_out)  # shape: (batch_size, num_classes)
        return logits

    @torch.no_grad()
    def predict_next_events(self, x, top_g=9):
        """
        Predicts the top_g most likely next event classes.
        """
        self.eval()
        logits = self.forward(x)
        probs = torch.softmax(logits, dim=-1)
        top_probs, top_indices = torch.topk(probs, top_g, dim=-1)
        return top_indices.cpu().numpy()

    def detect_anomalies_in_trace(self, indexed_sequence, window_size=10, top_g=9, device='cpu'):
        """
        Evaluates a single indexed sequence of event IDs and detects anomalies.
        Returns a dictionary containing is_anomaly, anomaly_count, and list of anomalous steps.
        """
        self.eval()
        n = len(indexed_sequence)
        anomalies = []
        
        # Prepare all sliding windows for the trace
        X = []
        y = []
        for i in range(n):
            target = indexed_sequence[i]
            start_idx = max(0, i - window_size)
            input_seq = indexed_sequence[start_idx:i]
            pad_len = window_size - len(input_seq)
            padded_input = [0] * pad_len + input_seq
            X.append(padded_input)
            y.append(target)
            
        if not X:
            return {"is_anomaly": False, "anomaly_count": 0, "anomalous_indices": []}
            
        # Convert to tensor and predict in batch
        X_tensor = torch.tensor(X, dtype=torch.long).to(device)
        logits = self.forward(X_tensor)
        probs = torch.softmax(logits, dim=-1)
        
        # Get top g predicted candidates for each step
        _, top_indices = torch.topk(probs, top_g, dim=-1)
        top_indices = top_indices.cpu().numpy()
        
        # Flag steps where the actual event is NOT in the top-g predictions
        for idx in range(n):
            actual = y[idx]
            candidates = top_indices[idx]
            if actual not in candidates:
                anomalies.append(idx)
                
        return {
            "is_anomaly": len(anomalies) > 0,
            "anomaly_count": len(anomalies),
            "anomalous_indices": anomalies
        }
