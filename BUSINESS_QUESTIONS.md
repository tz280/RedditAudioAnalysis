# Business Questions and Technical Approaches 

> **Project Focus:** Smart Audio Products & Wireless Earbuds Market Analysis 
>  
> This document presents a structured set of business questions designed to extract actionable consumer and market insights from large-scale Reddit discussions related to wireless earbuds and smart audio products. The analysis leverages Reddit comments and submissions data spanning from June 2023 to July 2024.

---
**Project:** Smart Audio Products Consumer Insights: A Big Data Analysis of Wireless Earbuds Market Dynamics  

**Team:** Team 23  

**Dataset:** Reddit Comments & Submissions (June 2023 – July 2024)  

**Subreddit Coverage:** Audio-focused and lifestyle communities including `headphones`, `audiophile`, `AirPods`, `sony`, `bose`, and related subreddits  

**High-Level Problem Statement:**  

The wireless audio market has experienced rapid expansion in recent years, with consumers facing increasingly complex choices across brands (e.g., Apple, Sony, Bose), price tiers ($20–$4000), and usage scenarios (gaming, fitness, commuting, professional production). This project applies large-scale Reddit data analytics to systematically uncover consumer preferences, key pain points, emotional reactions, and decision-making factors in the wireless audio ecosystem. The ultimate objective is to generate actionable insights to inform product design, marketing strategy, and competitive positioning.

---

## EDA – Question 1: How active are Reddit communities discussing wireless earbuds, and which subreddits drive the most engagement?

**Analysis Type:** Descriptive Analytics — Subreddit Engagement  

**Goal:**  
Quantify where wireless earbud discussions are most active on Reddit and identify which communities generate the highest levels of engagement, posting activity, and shared user participation.

**Technical Approach:**  
- Compute mean score, posting frequency, and total activity volume for all subreddits containing earbud-related content  
- Rank subreddits across three engagement metrics (activity volume, average score, posting frequency)  
- Construct a user-overlap matrix to measure cross-community audience mobility  
- Visualize top subreddits and shared-user networks using bar charts and heatmaps  

**Expected Insights:**  
- `audiophile` and `headphones` dominate all major engagement metrics and act as central hubs  
- `AirPods` leads all brand-focused subreddits in both activity and engagement intensity  
- General audio subreddits generate nearly twice the engagement of brand-specific communities  
- A strong long-tail pattern exists where most posts receive minimal engagement while a small subset drives virality  
- High shared-user overlap indicates fluid cross-community participation  

---

## EDA – Question 2: Which product communities attract the highest engagement and loyalty?

**Analysis Type:** EDA — Subreddit Comparison & Engagement Metrics  

**Goal:**  
Measure and compare engagement levels across brand-focused and general audio subreddits to determine which communities sustain the highest long-term participation and loyalty.

**Technical Approach:**  
- Compute average post score, number of comments, and posting frequency for each subreddit  
- Rank subreddits by mean engagement metrics  
- Measure user overlap to assess cross-brand participation behavior  

**Expected Insights:**  
- Identification of top-performing brand subreddits (e.g., `AirPods` vs. `SonyHeadphones`)  
- Differentiation between highly dedicated user bases and transient discussion-driven communities  
- Quantification of engagement inequality across communities  

---

## EDA – Question 3: How does post timing and structure relate to discussion depth?

**Analysis Type:** EDA — Post–Comment Relationship Analysis  

**Goal:**  
Analyze whether posting time and post-level metadata features systematically influence the volume and depth of user discussion.

**Technical Approach:**  
- Join `submissions` and `comments` datasets on `link_justid`  
- Compute correlations between post-level features (`score`, `upvote_ratio`, `title_length`, `created_utc`) and the number of comments  
- Compare engagement patterns between posts with and without media content (if available)  
- Visualize comment volume across posting-time bins and score distributions  

**Expected Insights:**  
- Identification of optimal posting times for triggering deep discussions  
- Quantification of how visibility metrics influence downstream engagement  
- Characterization of post quality versus longevity of discussion  

---

## NLP – Question 1: What features, pain points, and sentiments are most discussed about wireless earbuds?

**Analysis Type:** NLP — Feature-Based Text Mining & Sentiment Analysis  

**Goal:**  
Systematically identify which earbud features and product attributes users discuss most frequently, determine dominant pain points, and quantify sentiment polarity associated with each feature.

**Technical Approach:**  
- Apply global text cleaning including lowercase normalization, removal of `[deleted]`, `[removed]`, and very short comments  
- Implement a two-tier subreddit filtering strategy:  
  - Retain all content from earbud-focused subreddits (`headphones`, `AirPods`, `sony`, `bose`)  
  - Retain content from mixed subreddits (`android`, `apple`, `audiophile`) only when earbud-related keywords appear  
- Construct a 13-feature dictionary including `ANC`, `battery`, `sound_quality`, `comfort`, `mic_quality`, `connectivity`, `durability`, `water_resistance`, `multipoint`, `transparency`, `touch_controls`, `app`, `case`, and `price`  
- Map each feature to extensive keyword and regex variants  
- Perform regex-based feature extraction using Spark via a single-pass distributed scan  
- Apply VADER sentiment analysis to a large random sample of feature-matched comments  
- Estimate:  
  - Mean sentiment score per feature  
  - Proportion of positive, negative, and neutral mentions  
  - Major pain points based on strongly negative sentiment  

**Expected Insights:**  
- Identification of most frequently discussed features (e.g., sound quality, battery life, and price)  
- Detection of features generating the highest negative sentiment (e.g., durability and battery degradation)  
- Clear separation between performance-driven and reliability-driven discussions  
- Actionable input for product design prioritization and quality improvement  

---

## NLP – Question 2: What earbud brand dominates controversial discussion on Reddit?

**Analysis Type:** NLP — Brand-Oriented Sentiment and Feature Analysis  

**Goal:**  
Examine which earbud brands dominate online discussion volume and how sentiment distribution (positive, negative, neutral) varies by brand and feature.

**Technical Approach:**  
- Perform text preprocessing (lowercasing, whitespace normalization, noise removal)  
- Define 16 positive and 16 negative sentiment lexicon terms  
- Select 7 major earbud brands and 20 feature/problem-related keywords  
- Conduct brand-level sentiment and concern-feature cross-analysis  

**Expected Insights:**  
- Brand-specific sentiment distributions  
- Average text length by brand as a proxy for engagement intensity  
- Feature-driven versus problem-driven discussion profiles by brand  

---

## NLP – Question 3: What emotional reactions do Reddit users express toward different earbud features?

**Analysis Type:** NLP — Emotion Classification & Feature–Emotion Mapping  

**Goal:**  
Identify which earbud features trigger the strongest emotional reactions and how emotional responses differ across functional dimensions.

**Technical Approach:**  
- Apply multi-label emotion classification using six emotion categories: joy, trust, anger, sadness, fear, and disgust  
- Use the same 13-feature dictionary for feature extraction  
- Map emotional labels to feature mentions via regex-based extraction in Spark  
- Aggregate emotion intensities by feature  

**Expected Insights:**  
- Durability and connectivity exhibit peak anger and sadness due to reliability failures  
- Battery-related issues evoke softer disappointment dominated by sadness  
- Sound quality, ANC, and comfort generate the strongest joy and trust  
- Mic and transparency features yield mixed emotional responses  
- Price amplifies both trust and emotional backlash when expectations are violated  

---

## NLP – Question 4: How did public sentiment respond to the AirPods Pro USB-C launch?

**Analysis Type:** NLP — Temporal Sentiment Analysis & Topic Modeling  

**Goal:**  
Analyze sentiment dynamics, keyword trends, and dominant topics surrounding the AirPods Pro USB-C launch in September 2023.

**Technical Approach:**  
- Filter Reddit posts mentioning AirPods Pro from June 2023 to July 2024  
- Apply VADER sentiment analysis to track sentiment evolution over time  
- Track keyword frequencies for `USB-C`, `Lightning`, `upgrade`, `charging`, and related terms  
- Perform LDA topic modeling to extract dominant discussion themes  
- Compare pre-launch (Jun–Aug 2023) and post-launch (Sep 2023–Jul 2024) periods  
- Aggregate weekly and monthly discussion volumes  
- Compute sentiment distribution changes  

**Expected Insights:**  
- Quantitative measurement of community reception to the USB-C transition  
- Identification of keywords with the largest post-launch growth  
- Discovery of dominant discussion themes (ecosystem compatibility, upgrade value, etc.)  
- Characterization of sentiment spike and stabilization patterns  
- Assessment of whether incremental hardware updates generate sustained enthusiasm or upgrade fatigue  

---

## ML – Question 1: How well can we predict Reddit post engagement using only content at posting time?

**Analysis Type:** Machine Learning — Regression Modeling  

**Goal:**  
Quantify how much of a post’s final engagement (log-transformed score) can be predicted from content and timing alone, and estimate the incremental causal impact of early social feedback.

**Technical Approach:**  
- Define target variable as `log(1 + score)`  
- Construct two regression models:  
  - **Content-Only Model:** TF-IDF features from title and selftext, subreddit one-hot encoding, text length, posting hour, and day of week  
  - **Content + Early Comments Model:** All content features plus first-hour comment count, average early comment score, maximum early score, average comment length, and score variance  
- Train models using Spark-based distributed feature pipelines  
- Evaluate using R², RMSE, and MAE  
- Conduct feature-group importance analysis  

**Expected Insights:**  
- Upper bound of engagement predictability from content alone  
- Magnitude of the social snowball effect introduced by early interactions  
- Relative explanatory power of textual, temporal, and social feedback features  

---

## ML – Question 2: Can we classify Reddit post sentiment at scale?

**Analysis Type:** NLP + ML — Multiclass Classification  

**Goal:**  
Automatically classify post-level sentiment (positive, negative, neutral) to enable large-scale public opinion tracking.

**Technical Approach:**  
- Read Reddit comments and submissions  
- Apply regex-based cleaning (lowercase conversion, URL removal, special character filtering)  
- Implement keyword-based weak labeling using 15 positive and 16 negative words  
- Convert text into 1000-dimensional TF-IDF sparse vectors  
- Train multiclass classification models  
- Evaluate using precision, recall, F1-score, and accuracy  

**Expected Insights:**  
- Overall sentiment classification accuracy  
- Practical feasibility of automated sentiment monitoring at scale  

---

## ML – Question 3: Can we identify inauthentic promotional accounts?

**Analysis Type:** ML — Behavioral Binary Classification  

**Goal:**  
Detect bots and promotional accounts to ensure the authenticity and reliability of consumer insights.

**Technical Approach:**  
- Aggregate 14 behavioral features for 145,234 users across four dimensions:  
  - Posting volume  
  - Community diversity  
  - Temporal posting patterns  
  - Engagement quality and text patterns  
- Generate rule-based pseudo-labels for suspicious behavior  
- Train a Random Forest classifier (100 trees, max depth = 8)  
- Evaluate using Accuracy, Precision, Recall, F1, AUC-ROC, and AUC-PR  
- Conduct feature importance analysis  

**Expected Insights:**  
- Behavioral signals most indicative of inauthentic activity  
- Estimated bot contamination rate in earbud discussions  
- Assessment of temporal versus content-based detection effectiveness  

---

## ML – Question 4: What underlying pain-point clusters exist in earbud discussions?

**Analysis Type:** Unsupervised Learning — K-Means Pain-Point Clustering  

**Goal:**  
Discover natural groupings of user complaints based on seven key pain-point dimensions and quantify how these clusters differ across subreddits.

**Technical Approach:**  
- Construct seven-dimensional pain-point vectors (`sound`, `battery`, `ANC`, `comfort`, `durability`, `connectivity`, `price`) for 147,000+ comments  
- Evaluate optimal cluster number using the Elbow Method and Silhouette Score  
- Select `k = 9` based on maximum silhouette performance  
- Fit final K-Means model  
- Visualize cluster centroids and subreddit-level cluster distributions  
- Compare dominant complaint types across communities  

**Expected Insights:**  
- Each subreddit exhibits a distinct “complaint fingerprint”  
- Technical subreddits emphasize connectivity and performance  
- Audiophile communities prioritize sound quality  
- Value-focused users emphasize durability and long-term quality  
- Clear segmentation of expectation tiers across consumer groups  

---
