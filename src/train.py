import os
import time
import argparse
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import TensorDataset, DataLoader
from tqdm import tqdm
import mlflow
import mlflow.pytorch
from sklearn.metrics import precision_recall_fscore_support

from preprocess import prepare_dataset
from model import DeepLogLSTM

def parse_args():
    parser = argparse.ArgumentParser(description="Train DeepLog LSTM Model on HDFS dataset")
    parser.add_argument("--data_path", type=str, default="../HDFS_v1/preprocessed/Event_traces.csv", 
                        help="Path to the preprocessed event traces CSV")
    parser.add_argument("--is_preprocessed", type=bool, default=True,
                        help="Whether using the original preprocessed CSV format")
    parser.add_argument("--max_traces", type=int, default=20000,
                        help="Maximum number of traces to load to speed up local training (0 to load all)")
    parser.add_argument("--window_size", type=int, default=10, help="Sliding window size")
    parser.add_argument("--batch_size", type=int, default=512, help="Batch size for training")
    parser.add_argument("--epochs", type=int, default=5, help="Number of training epochs")
    parser.add_argument("--lr", type=float, default=0.001, help="Learning rate")
    parser.add_argument("--embedding_dim", type=int, default=64, help="Embedding dimension")
    parser.add_argument("--hidden_size", type=int, default=64, help="LSTM hidden size")
    parser.add_argument("--num_layers", type=int, default=2, help="Number of LSTM layers")
    parser.add_argument("--top_g", type=int, default=9, help="Number of top candidates for anomaly checking")
    parser.add_argument("--device", type=str, default="cuda" if torch.cuda.is_available() else "cpu", 
                        help="Training device (cuda or cpu)")
    parser.add_argument("--experiment_name", type=str, default="DeepLog-HDFS", help="MLflow experiment name")
    return parser.parse_args()

def evaluate_model(model, test_seqs, test_labels, window_size, top_g, device):
    """
    Evaluates DeepLog LSTM on the test sequences.
    Traces with at least one prediction step outside top_g candidates are marked as Anomaly (1).
    """
    print(f"Evaluating model on {len(test_seqs)} test traces with top_g={top_g}...")
    y_true = np.array(test_labels)
    y_pred = []
    
    # Run prediction for each sequence
    for seq in tqdm(test_seqs, desc="Evaluating traces"):
        result = model.detect_anomalies_in_trace(seq, window_size=window_size, top_g=top_g, device=device)
        y_pred.append(1 if result["is_anomaly"] else 0)
        
    y_pred = np.array(y_pred)
    
    # Calculate metrics
    precision, recall, f1, _ = precision_recall_fscore_support(y_true, y_pred, average='binary', zero_division=0)
    
    # Calculate confusion matrix components
    tp = np.sum((y_true == 1) & (y_pred == 1))
    fp = np.sum((y_true == 0) & (y_pred == 1))
    fn = np.sum((y_true == 1) & (y_pred == 0))
    tn = np.sum((y_true == 0) & (y_pred == 0))
    
    metrics = {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "tp": int(tp),
        "fp": int(fp),
        "fn": int(fn),
        "tn": int(tn)
    }
    
    print(f"\nEvaluation Results (top_g={top_g}):")
    print(f"  Precision : {precision:.4f}")
    print(f"  Recall    : {recall:.4f}")
    print(f"  F1-Score  : {f1:.4f}")
    print(f"  Confusion Matrix: TP={tp}, FP={fp}, FN={fn}, TN={tn}")
    
    return metrics

def main():
    args = parse_args()
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Resolve absolute paths
    data_path = os.path.abspath(os.path.join(base_dir, args.data_path))
    vocab_path = os.path.join(base_dir, "vocab.json")
    model_save_path = os.path.join(base_dir, "../models/deeplog_lstm.pth")
    
    # Check if dataset path exists
    if not os.path.exists(data_path):
        # Fallback to local preprocessed_sample.csv if available
        fallback_path = os.path.abspath(os.path.join(base_dir, "../preprocessed_sample.csv"))
        if os.path.exists(fallback_path):
            print(f"Dataset not found at {data_path}. Falling back to sample dataset {fallback_path}...")
            data_path = fallback_path
            args.is_preprocessed = False
        else:
            raise FileNotFoundError(f"No dataset file found at {data_path} or {fallback_path}. Run parser.py first!")

    # Load and limit dataset size if needed
    if args.max_traces > 0:
        # Load a subset of traces to keep it fast locally
        df = pd.read_csv(data_path)
        if len(df) > args.max_traces:
            print(f"Limiting dataset from {len(df)} to {args.max_traces} traces to speed up training...")
            df = df.sample(n=args.max_traces, random_state=42).reset_index(drop=True)
            # Temporarily save subset to preprocess
            temp_path = os.path.join(base_dir, "temp_subset.csv")
            df.to_csv(temp_path, index=False)
            data_path = temp_path
            
    # Prepare data
    X_train, y_train, test_seqs, test_labels, indexer = prepare_dataset(
        csv_path=data_path,
        vocab_path=vocab_path,
        window_size=args.window_size,
        is_preprocessed_file=args.is_preprocessed
    )
    
    # Clean up temp file if created
    if args.max_traces > 0 and os.path.exists(os.path.join(base_dir, "temp_subset.csv")):
        os.remove(os.path.join(base_dir, "temp_subset.csv"))
        
    num_classes = indexer.num_events
    
    # Create DataLoader
    train_dataset = TensorDataset(
        torch.tensor(X_train, dtype=torch.long),
        torch.tensor(y_train, dtype=torch.long)
    )
    train_loader = DataLoader(train_dataset, batch_size=args.batch_size, shuffle=True, drop_last=True)
    
    # Initialize MLflow
    mlflow.set_experiment(args.experiment_name)
    
    with mlflow.start_run() as run:
        print(f"MLflow Run Started. ID: {run.info.run_id}")
        
        # Log Hyperparameters
        params = {
            "num_classes": num_classes,
            "window_size": args.window_size,
            "batch_size": args.batch_size,
            "epochs": args.epochs,
            "learning_rate": args.lr,
            "embedding_dim": args.embedding_dim,
            "hidden_size": args.hidden_size,
            "num_layers": args.num_layers,
            "top_g": args.top_g,
            "optimizer": "Adam",
            "loss_function": "CrossEntropyLoss"
        }
        mlflow.log_params(params)
        
        # Initialize Model, Loss, Optimizer
        model = DeepLogLSTM(
            num_classes=num_classes,
            embedding_dim=args.embedding_dim,
            hidden_size=args.hidden_size,
            num_layers=args.num_layers
        ).to(args.device)
        
        criterion = nn.CrossEntropyLoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=args.lr)
        
        # Training loop
        print(f"Starting training on {args.device} for {args.epochs} epochs...")
        model.train()
        
        for epoch in range(1, args.epochs + 1):
            epoch_loss = 0.0
            epoch_start_time = time.time()
            
            progress_bar = tqdm(train_loader, desc=f"Epoch {epoch}/{args.epochs}")
            for X_batch, y_batch in progress_bar:
                X_batch, y_batch = X_batch.to(args.device), y_batch.to(args.device)
                
                optimizer.zero_grad()
                logits = model(X_batch)
                loss = criterion(logits, y_batch)
                loss.backward()
                optimizer.step()
                
                epoch_loss += loss.item() * X_batch.size(0)
                progress_bar.set_postfix({"loss": loss.item()})
                
            average_loss = epoch_loss / len(train_dataset)
            elapsed_time = time.time() - epoch_start_time
            
            # Log epoch loss to MLflow
            mlflow.log_metric("train_loss", average_loss, step=epoch)
            mlflow.log_metric("epoch_time_seconds", elapsed_time, step=epoch)
            
            print(f"Epoch {epoch} summary: Average Loss = {average_loss:.4f}, Time = {elapsed_time:.1f}s")
            
        # Create directory to save model locally
        os.makedirs(os.path.dirname(model_save_path), exist_ok=True)
        torch.save(model.state_dict(), model_save_path)
        print(f"Saved local model weights to {model_save_path}")
        
        # Evaluate model on test set
        eval_metrics = evaluate_model(
            model=model, 
            test_seqs=test_seqs, 
            test_labels=test_labels, 
            window_size=args.window_size, 
            top_g=args.top_g, 
            device=args.device
        )
        
        # Log evaluation metrics to MLflow
        mlflow.log_metrics(eval_metrics)
        
        # Log vocab mapping as an artifact
        mlflow.log_artifact(vocab_path)
        
        # Log PyTorch Model in MLflow
        # Note: We pass standard input example to register signature
        input_example = X_train[:5].astype(np.int64)
        mlflow.pytorch.log_model(
            pytorch_model=model, 
            artifact_path="deeplog_lstm_model",
            input_example=input_example
        )
        
        print("\nTraining and Evaluation completed successfully. Artifacts logged to MLflow.")

if __name__ == '__main__':
    main()
