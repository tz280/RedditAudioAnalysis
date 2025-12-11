#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Painpoint Clustering (scikit-learn)
Author: Team 23

Steps:
1. Load painpoint_vectors_combined.csv (comments + submissions)
2. Build painpoint matrix
3. Find optimal k (elbow + silhouette)
4. Run k-means clustering
5. Interpret clusters (centroids)
6. Project clusters back to subreddits
7. Save heatmaps, centroids, metrics
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.utils import resample

# ============================== PATHS ==============================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CSV_DIR = Path("/home/ubuntu/fall-2025-project-team23/data/csv")
PLOTS_DIR = Path("/home/ubuntu/fall-2025-project-team23/data/plots")
TXT_DIR = Path("/home/ubuntu/fall-2025-project-team23/data/txt")

PLOTS_DIR.mkdir(parents=True, exist_ok=True)
TXT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_CSV = CSV_DIR / "painpoint_vectors_combined.csv"

PLOTS_DIR.mkdir(parents=True, exist_ok=True)
TXT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_CSV = CSV_DIR / "painpoint_vectors_combined.csv"

# ============================== LOAD DATA ==============================

print("Loading:", INPUT_CSV)
df = pd.read_csv(INPUT_CSV)

PAIN_COLS = ["battery", "anc", "comfort", "price", "durability", "connectivity", "sound"]

# Cast to float32 for speed + lower memory
X = df[PAIN_COLS].astype("float32").values

print("Dataset shape (n_samples, n_features):", X.shape)

n_samples, n_features = X.shape

# ============================== ELBOW + SILHOUETTE ==============================

K_RANGE = range(2, 10)
inertias = []
sil_scores = []

# For very large datasets, compute silhouette on a subsample for speed
MAX_SIL_SAMPLES = 50000
if n_samples > MAX_SIL_SAMPLES:
    print(f"Using a subsample of {MAX_SIL_SAMPLES} points for silhouette scoring.")
    X_sil = resample(
        X,
        n_samples=MAX_SIL_SAMPLES,
        replace=False,
        random_state=42,
    )
else:
    X_sil = X

for k in K_RANGE:
    print(f"Fitting KMeans for k={k} ...")
    model = KMeans(n_clusters=k, random_state=42, n_init="auto")
    model.fit(X)

    # Elbow: inertia from fitted model
    inertias.append(model.inertia_)

    # Silhouette: use full data if small, subsample otherwise
    if X_sil is X:
        labels_for_sil = model.labels_
        sil = silhouette_score(X, labels_for_sil)
    else:
        labels_for_sil = model.predict(X_sil)
        sil = silhouette_score(X_sil, labels_for_sil)

    sil_scores.append(sil)

# Plot Elbow
plt.figure(figsize=(8, 5))
plt.plot(list(K_RANGE), inertias, marker="o")
plt.title("Elbow Method for Optimal K")
plt.xlabel("k")
plt.ylabel("Inertia")
plt.tight_layout()
elbow_path = PLOTS_DIR / "clustering_elbow_combined.png"
plt.savefig(elbow_path, dpi=300)
plt.close()
print("Saved elbow plot →", elbow_path)

# Plot Silhouette
plt.figure(figsize=(8, 5))
plt.plot(list(K_RANGE), sil_scores, marker="o")
plt.title("Silhouette Scores for k")
plt.xlabel("k")
plt.ylabel("Score")
plt.tight_layout()
sil_path = PLOTS_DIR / "clustering_silhouette_combined.png"
plt.savefig(sil_path, dpi=300)
plt.close()
print("Saved silhouette plot →", sil_path)

# BEST K by silhouette
BEST_K = list(K_RANGE)[sil_scores.index(max(sil_scores))]
print("Best k (by silhouette) =", BEST_K)

# ============================== FIT FINAL KMEANS ==============================

final_model = KMeans(n_clusters=BEST_K, random_state=42, n_init="auto")
df["cluster"] = final_model.fit_predict(X)

# ============================== CLUSTER CENTROIDS ==============================

centers = pd.DataFrame(final_model.cluster_centers_, columns=PAIN_COLS)
centers.index = [f"Cluster {i}" for i in centers.index]

centers_path = CSV_DIR / "cluster_centroids.csv"
centers.to_csv(centers_path, index=True)
print("Saved cluster centroids →", centers_path)

plt.figure(figsize=(10, 6))
sns.heatmap(centers, annot=True, cmap="Blues")
plt.title("Cluster Centers (Painpoint Importance)")
plt.tight_layout()
centers_plot_path = PLOTS_DIR / "cluster_centers_combined.png"
plt.savefig(centers_plot_path, dpi=300)
plt.close()
print("Saved cluster centers heatmap →", centers_plot_path)

# ============================== SUBREDDIT PROJECTION ==============================

sub_cluster = (
    df.groupby(["subreddit", "cluster"])["text"]
      .count()
      .reset_index(name="count")
)

pivot = sub_cluster.pivot(index="subreddit", columns="cluster", values="count").fillna(0)

# convert counts → percentages per subreddit
pivot_pct = pivot.div(pivot.sum(axis=1), axis=0)

plt.figure(figsize=(12, 14))
sns.heatmap(pivot_pct, cmap="magma", annot=False)
plt.title("Subreddit Distribution Across Painpoint Clusters")
plt.tight_layout()
subreddit_heatmap_path = PLOTS_DIR / "subreddit_cluster_heatmap_combined.png"
plt.savefig(subreddit_heatmap_path, dpi=300)
plt.close()

print("Saved subreddit × cluster heatmap →", subreddit_heatmap_path)
print("All clustering visualizations saved to:", PLOTS_DIR)
