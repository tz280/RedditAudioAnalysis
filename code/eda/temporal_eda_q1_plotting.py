import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# ---- style ----
sns.set_theme(style="whitegrid", context="talk", font_scale=1.1)
plt.rcParams.update({
    "figure.dpi": 300,
    "axes.labelweight": "bold",
    "axes.titleweight": "bold",
    "axes.titlesize": 16,
    "axes.labelsize": 13,
    "legend.title_fontsize": 12,
    "legend.fontsize": 11,
})
Path("data/plots").mkdir(parents=True, exist_ok=True)

# ---- paths based on your screenshot ----
p_hour_overall = "data/csv/eda_temporal_by_hour_overall/eda_temporal_by_hour_overall.csv"
p_hour_sub     = "data/csv/eda_temporal_by_hour_subreddit/eda_temporal_by_hour_subreddit.csv"
p_ymdow_over   = "data/csv/eda_temporal_by_y_m_dow_overall/eda_temporal_by_y_m_dow_overall.csv"
p_ymdow_sub    = "data/csv/eda_temporal_by_y_m_dow_subreddit/eda_temporal_by_y_m_dow_subreddit.csv"






import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Weekly % per subreddit
df_week_sub = pd.read_csv("data/csv/eda_temporal_by_y_m_dow_subreddit/eda_temporal_by_y_m_dow_subreddit.csv")

# collapse to total per (subreddit, dow), then normalize within subreddit
wk = (df_week_sub.groupby(["subreddit","dow"], as_index=False)["post_count"].sum())
wk["pct"] = wk["post_count"] / wk.groupby("subreddit")["post_count"].transform("sum") * 100

# pivot to dow x subreddit (%), ensure dow order 1..7
wk_piv = wk.pivot(index="dow", columns="subreddit", values="pct").sort_index()

plt.figure(figsize=(12,6))
for col in wk_piv.columns:
    plt.plot(wk_piv.index, wk_piv[col], marker="o", linewidth=2, label=col)
plt.title("Weekly Distribution Within Each Subreddit (%)")
plt.xlabel("Day of Week (1=Sun … 7=Sat)")
plt.ylabel("Percent of That Subreddit’s Weekly Posts")
plt.legend(title="Subreddit", bbox_to_anchor=(1.02,1), loc="upper left")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("data/plots/eda_weekly_within_subreddit_percent.png", dpi=300)
plt.close()


# Hourly % per subreddit
df_hour_sub = pd.read_csv("data/csv/eda_temporal_by_hour_subreddit/eda_temporal_by_hour_subreddit.csv")

hr = (df_hour_sub.groupby(["subreddit","hour"], as_index=False)["post_count"].sum())
hr["pct"] = hr["post_count"] / hr.groupby("subreddit")["post_count"].transform("sum") * 100

hr_piv = hr.pivot(index="hour", columns="subreddit", values="pct").sort_index()

plt.figure(figsize=(12,6))
for col in hr_piv.columns:
    plt.plot(hr_piv.index, hr_piv[col], marker="o", linewidth=2, label=col)
plt.title("Hourly Distribution Within Each Subreddit (%)")
plt.xlabel("Hour of Day (UTC)")
plt.ylabel("Percent of That Subreddit’s Daily Posts")
plt.legend(title="Subreddit", bbox_to_anchor=(1.02,1), loc="upper left")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("data/plots/eda_hourly_within_subreddit_percent.png", dpi=300)
plt.close()



#Weekly Posting Activity (Absolute Counts per Subreddit)

df = pd.read_csv("data/csv/eda_temporal_by_y_m_dow_subredditnotech/eda_temporal_by_y_m_dow_subredditnotech.csv")

plt.figure(figsize=(10,6))
sns.lineplot(data=df, x='dow', y='post_count', hue='subreddit', marker='o', ci=None)
plt.title("Weekly Posting Activity — Audio Subreddits")
plt.xlabel("Day of Week (1=Sun, 7=Sat)")
plt.ylabel("Total Comment Count")
plt.legend(title="Subreddit", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.savefig("data/plots/eda_weekly_comment_count_by_subreddit.png", dpi=300)
plt.show()


#Hourly Posting Activity (Absolute Counts per Subreddit)


df_hour = pd.read_csv("data/csv/eda_temporal_by_hour_subredditnotech/eda_temporal_by_hour_subredditnotech.csv")

plt.figure(figsize=(10,6))
sns.lineplot(data=df_hour, x='hour', y='post_count', hue='subreddit', marker='o')
plt.title("Hourly Posting Activity — Audio Subreddits")
plt.xlabel("Hour of Day (UTC)")
plt.ylabel("Total Comment Count")
plt.legend(title="Subreddit", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.savefig("data/plots/eda_hourly_comment_count_by_subreddit.png", dpi=300)
plt.show()



# bar chart: total comment share by subreddit

import pandas as pd
import matplotlib.pyplot as plt

# Load and prep data
df_pie = pd.read_csv("data/csv/eda_temporal_by_hour_subredditnotech/eda_temporal_by_hour_subredditnotech.csv")
sub_sum = (
    df_pie.groupby("subreddit", as_index=False)["post_count"]
          .sum()
          .sort_values("post_count", ascending=True)
)

total = sub_sum["post_count"].sum()
sub_sum["pct"] = 100 * sub_sum["post_count"] / total

# --- Plot ---
fig, ax = plt.subplots(figsize=(8, 5))
bars = ax.barh(sub_sum["subreddit"], sub_sum["post_count"], color="#4C72B0")

# Labels: total + percentage
ax.bar_label(
    bars,
    labels=[f"{v:,.0f}  ({p:.1f}%)" for v, p in zip(sub_sum["post_count"], sub_sum["pct"])],
    padding=3,
    fontsize=9,
    color="black"
)


ax.set_xlabel("Total Comments", fontsize=10)
ax.set_ylabel("")  # cleaner
ax.set_title("Total Comment Volume by Subreddit", fontsize=13, weight="bold", pad=10)


ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.spines["left"].set_color("#BBBBBB")
ax.spines["bottom"].set_color("#BBBBBB")
ax.tick_params(axis="x", labelsize=9)
ax.tick_params(axis="y", labelsize=9)

plt.tight_layout()
plt.savefig("data/plots/eda_total_comment_share_bar_clean.png", dpi=300)
plt.close()
