# FINTEL — LangGraph Agent Graph Design

**Version:** 1.0  
**Created:** 2026-06-30  
**Purpose:** Single source of truth for M3 implementation. Every node, edge, and state field defined here before a single line of code is written.

---

## 1. Pipeline Trigger Points

Five GitHub Actions cron jobs trigger the pipeline per day (times in IST):

| Session Name | IST Time | UTC Cron        | What it covers                         |
| ------------ | -------- | --------------- | -------------------------------------- |
| NSE_OPEN     | 09:00    | `37 3 * * 1-5`  | Pre-market India news, overnight US    |
| NSE_MID      | 12:00    | `37 6 * * 1-5`  | Mid-session India pulse                |
| NSE_CLOSE    | 15:30    | `7 10 * * 1-5`  | India close, FII/DII data, day summary |
| US_OPEN      | 19:00    | `30 13 * * 1-5` | US pre-market, macro data              |
| US_CLOSE     | 01:00    | `37 13 * * 1-5` | US close summary, after-hours news     |
| SUPERVISOR   | 23:00    | `30 19 * * 0`   | Sunday night weekly calibration        |

---

## 2. AgentState Schema

This is the single typed object that flows through every node. Every agent reads from it and writes back to it.

```py
AgentState:
  # Session metadata
  session_id:          str          # UUID, generated at trigger
  session_type:        str          # NSE_OPEN | NSE_MID | NSE_CLOSE | US_OPEN | US_CLOSE | SUPERVISOR
  triggered_at:        datetime     # UTC timestamp of GitHub Actions trigger
  market_focus:        str          # "india" | "us" | "global" (derived from session_type)

  # Raw collection output (CollectorAgent writes)
  raw_items:           List[RawItem]

  # After deduplication (DeduplicatorAgent writes)
  deduplicated_items:  List[NewsItem]

  # After relevance filter (RelevanceFilterAgent writes)
  filtered_items:      List[NewsItem]

  # After noise scoring (NoiseScorerAgent writes)
  scored_items:        List[ScoredNewsItem]

  # After validation (ValidatorAgent writes)
  validated_items:     List[ValidatedNewsItem]

  # Items routed to deep check (ValidatorAgent flags, DeepCheckAgent resolves)
  pending_deep_check:  List[ValidatedNewsItem]
  deep_checked_items:  List[ValidatedNewsItem]

  # Market data (FundamentalsAgent + TechnicalAgent write)
  fundamentals:        Dict[ticker_yf, FundamentalsData]
  technicals:          Dict[ticker_yf, TechnicalsData]
  fii_dii_data:        FIIDIIData | None   # India sessions only

  # After tagging (TaggerAgent writes)
  tagged_items:        List[TaggedNewsItem]

  # Signals (SignalGeneratorAgent writes)
  signals:             List[Signal]

  # Digest (SynthesizerAgent writes)
  digest_html:         str | None
  subject_line:        str | None

  # Delivery (DeliveryAgent writes)
  email_sent:          bool
  no_email_reason:     str | None   # "no_new_content" | "smtp_error" | None

  # Run tracking
  errors:              List[str]    # Non-fatal errors appended by any agent
  run_log:             List[str]    # Step completion log
  supabase_run_id:     str | None   # Written after pipeline_runs insert
```

---

## 3. Data Type Definitions

```py
RawItem:
  source_id:           str          # matches sources.json id field
  source_tier:         int          # 1 | 2 | 3 | 4
  raw_text:            str          # headline + snippet
  url:                 str
  published_at:        datetime | None
  fetched_at:          datetime

NewsItem (extends RawItem):
  item_hash:           str          # SHA-256 of headline + source_id
  corroboration_count: int          # how many sources carried same story
  corroborating_sources: List[str]  # source_ids that matched

ScoredNewsItem (extends NewsItem):
  noise_score:         float        # 0.0 to 10.0, higher = more signal

ValidatedNewsItem (extends ScoredNewsItem):
  trust_label:         str          # CONFIRMED | UNCONFIRMED | SINGLE_SOURCE | VERIFIED_DEEP | SINGLE_SOURCE_UNVERIFIED

TaggedNewsItem (extends ValidatedNewsItem):
  matched_tickers:     List[str]    # ticker_yf values matched from watchlist
  matched_sectors:     List[str]    # sector names matched
  peer_tickers:        List[str]    # peers of matched tickers
  time_horizon:        str          # SHORT_TERM | LONG_TERM | BOTH
  horizon_reason:      str          # why this horizon was assigned

FundamentalsData:
  ticker_yf:           str
  pe_ratio:            float | None
  pb_ratio:            float | None
  revenue_growth_yoy:  float | None  # percentage
  net_margin:          float | None  # percentage
  debt_to_equity:      float | None
  sector_avg_pe:       float | None
  vs_sector_pe:        str          # "CHEAP" | "FAIR" | "EXPENSIVE"
  fetched_at:          datetime
  cache_fresh:         bool         # True if pulled this week

TechnicalsData:
  ticker_yf:           str
  rsi_14:              float | None
  macd_value:          float | None
  macd_signal:         float | None
  macd_histogram:      float | None
  ema_9:               float | None
  ema_20:              float | None
  ema_50:              float | None
  ema_200:             float | None
  volume_today:        int | None
  volume_avg_20d:      int | None
  volume_ratio:        float | None   # today/avg — >1.5 = notable volume
  week_52_high:        float | None
  week_52_low:         float | None
  price_vs_52w_high:   float | None   # % below 52W high
  atr_14:              float | None   # volatility measure
  fetched_at:          datetime

FIIDIIData:
  date:                str
  fii_net:             float          # positive = net buy, negative = net sell (in crores)
  dii_net:             float
  fii_buy:             float
  fii_sell:            float
  dii_buy:             float
  dii_sell:            float
  market_sentiment:    str           # "BULLISH" | "BEARISH" | "NEUTRAL" based on flows

Signal:
  signal_id:           str           # UUID
  ticker_yf:           str
  ticker_display:      str
  signal_type:         str           # BULLISH_SHORT | BULLISH_LONG | BEARISH_SHORT | BEARISH_LONG | INTRADAY_WATCH
  confidence:          int           # 0 to 100
  time_horizon_days:   str           # "1-5" | "7-30" | "30-90" | "1-2 sessions"
  reasons:             List[str]     # max 3 bullet reasons
  supporting_news:     List[str]     # item hashes that contributed
  sector:              str
  affected_peers:      List[str]     # peer tickers also likely affected
  generated_at:        datetime
  session_id:          str
```

---

## 4. Agent Definitions

### Node 1: CollectorAgent

**Responsibility:** Fetch raw data from all sources applicable to this session.  
**Reads from state:** `session_type`, `market_focus`  
**Writes to state:** `raw_items`, appends to `run_log`  
**Logic:**

- Look up `sources.json`, filter sources whose `poll_sessions` includes current `session_type`
- For each source:
  - `rss` type → feedparser.parse(url)
  - `api` type → requests.get(endpoint) with API key from env
  - `scraper` type → requests.Session() with browser headers
  - `reddit_api` type → praw.Reddit().subreddit(name).hot(limit=n)
- On per-source error: append error to `state.errors`, skip that source, continue
- Outputs all raw items into `state.raw_items`

---

### Node 2: DeduplicatorAgent

**Responsibility:** Eliminate duplicates, cluster same-story items.  
**Reads from state:** `raw_items`  
**Writes to state:** `deduplicated_items`, appends to `run_log`  
**Logic:**

- For each raw item: compute `item_hash` = SHA-256(headline.lower().strip() + source_id)
- Query Supabase `news_items` table: SELECT item_hash WHERE item_hash = ? AND fetched_at > NOW() - 72h
- If exists in Supabase: skip (already processed)
- Cluster near-duplicates: items with headline text similarity > 0.85 (using difflib.SequenceMatcher) are merged into one item with `corroboration_count` incremented and `corroborating_sources` populated
- Output unique items into `state.deduplicated_items`

---

### Node 3: RelevanceFilterAgent

**Responsibility:** Hard-drop anything that doesn't match the watchlist.  
**Reads from state:** `deduplicated_items`  
**Writes to state:** `filtered_items`, appends to `run_log`  
**Logic:**

- Load watchlist.json keywords list for all tickers + sector names
- For each item: check if any keyword appears in (headline + snippet) — case-insensitive
- If no keyword match: DROP. No LLM call. Hard filter.
- If match: pass through to `state.filtered_items`
- Log: "Filtered X items to Y relevant items"

---

### Node 4: NoiseScorerAgent

**Responsibility:** Score each item 0-10 for signal quality.  
**Reads from state:** `filtered_items`  
**Writes to state:** `scored_items` (sorted by score descending), appends to `run_log`  
**Scoring Rules (additive):**

| Condition                                    | Score Adjustment |
| -------------------------------------------- | ---------------- |
| Source is Tier 1                             | +4               |
| Source is Tier 2                             | +2               |
| Source is Tier 3                             | +1               |
| Source is Tier 4                             | +0               |
| corroboration_count >= 3                     | +2               |
| corroboration_count == 2                     | +1               |
| Item contains specific numbers/percentages   | +1               |
| Item references a filing or official release | +1               |
| Vague/sensational language detected*         | -2               |
| Anonymous source mentioned                   | -1               |
| Ticker has risk_flag in watchlist            | -1               |

*Sensational keywords list: ["skyrocket", "crash", "explode", "moon", "must buy", "hot tip", "secret"]

Final score capped 0-10. Items with score < 2.0 are dropped (noise floor).

---

### Node 5: ValidatorAgent

**Responsibility:** Assign trust labels based on tier rules.  
**Reads from state:** `scored_items`  
**Writes to state:** `validated_items`, `pending_deep_check`, appends to `run_log`  
**Logic:**

- CONFIRMED: source_tier == 1, OR corroboration_count >= 2 with at least one Tier 2 source
- SINGLE_SOURCE: corroboration_count == 1 AND source_tier >= 3
- UNCONFIRMED: corroboration_count >= 2 but no Tier 1/2 source, OR single Tier 2 source
- Items with trust_label == SINGLE_SOURCE: move to `state.pending_deep_check`
- All others: add to `state.validated_items`

---

### Node 6: DeepCheckAgent

**Responsibility:** Attempt to verify SINGLE_SOURCE items via Gemini.  
**Reads from state:** `pending_deep_check`  
**Writes to state:** adds resolved items to `validated_items`, appends to `run_log`  
**Logic:**

- For each item in `pending_deep_check`:
  - Call Gemini 1.5 Flash with prompt: "Is this news headline corroborated by any other source? Headline: '{headline}'. Search for this story and tell me: (a) corroborated yes/no, (b) source name if yes. Respond in JSON: {corroborated: bool, source: str|null}"
  - If corroborated == True: update trust_label to VERIFIED_DEEP, add to `validated_items`
  - If corroborated == False: update trust_label to SINGLE_SOURCE_UNVERIFIED, still add to `validated_items` (but flagged)
- Append all resolved items to `state.validated_items`

---

### Node 7: FundamentalsAgent

**Responsibility:** Pull and cache fundamentals data for all watchlist tickers.  
**Reads from state:** `session_type`  
**Writes to state:** `fundamentals`, appends to `run_log`  
**Logic:**

- Check Supabase `fundamentals_cache`: for each ticker, is cache_fresh (fetched within 7 days)?
- If fresh: load from Supabase into `state.fundamentals`
- If stale: fetch via yfinance ticker.info — PE, PB, revenue growth, margins, debt/equity
- Compute `vs_sector_pe`: compare to pre-loaded sector averages (hardcoded dict, updated quarterly)
- Write fresh data back to Supabase `fundamentals_cache`
- **Only runs on NSE_CLOSE and US_CLOSE sessions** (not mid-session, data doesn't change intraday)

---

### Node 8: TechnicalAgent

**Responsibility:** Pull current technical indicator values per ticker.  
**Reads from state:** `session_type`, `market_focus`  
**Writes to state:** `technicals`, `fii_dii_data`, appends to `run_log`  
**Logic:**

- For India tickers: compute RSI, MACD, EMAs locally from yfinance OHLCV (avoids Alpha Vantage call budget)
- For US tickers: call Alpha Vantage API (budget: max 10 calls/session, batch where possible)
- Volume ratio: today_volume / avg_20day_volume (from yfinance)
- 52W high/low proximity: (current_price - 52w_low) / (52w_high - 52w_low)
- On NSE_CLOSE session only: fetch FII/DII data from NSE API, write to `state.fii_dii_data`
- Write all data to Supabase `technical_cache`

---

### Node 9: TaggerAgent

**Responsibility:** Connect news items to tickers, sectors, peers, and time horizons.  
**Reads from state:** `validated_items`, `fundamentals`, `technicals`  
**Writes to state:** `tagged_items`, appends to `run_log`  
**Logic:**

- For each validated item: match keywords against watchlist → populate `matched_tickers`
- From matched_tickers: look up sector and peers → populate `matched_sectors`, `peer_tickers`
- Assign `time_horizon`:
  - SHORT_TERM: item is earnings result, analyst upgrade/downgrade, product launch, regulatory fine
  - LONG_TERM: item is policy change, capex announcement, sector tailwind, M&A
  - BOTH: macro data (interest rates, inflation, GDP) or major structural change
- Write `tagged_items`

---

### Node 10: SignalGeneratorAgent

**Responsibility:** Combine news + fundamentals + technicals into directional signals per ticker.  
**Reads from state:** `tagged_items`, `fundamentals`, `technicals`, `fii_dii_data`  
**Writes to state:** `signals`, appends to `run_log`  
**Logic:**

- Group `tagged_items` by ticker
- For each ticker with >= 1 tagged item:
  - Get current RSI, MACD, EMA position, volume ratio from `technicals`
  - Get PE vs sector from `fundamentals`
  - Apply signal rules (all rules must pass for signal to fire):

  **BULLISH_SHORT** (confidence 60-90):
  - Positive catalyst (earnings beat, product launch, upgrade) AND
  - RSI < 70 (not overbought) AND
  - Price above EMA20 OR MACD histogram turning positive
  - Confidence boosted +10 if volume_ratio > 1.5

  **BULLISH_LONG** (confidence 55-85):
  - Positive fundamental catalyst (margin expansion, revenue beat, new large order) AND
  - PE < sector_avg_pe (reasonably valued) AND
  - FII_net positive on India session (if applicable)

  **BEARISH_SHORT** (confidence 60-90):
  - Negative catalyst (earnings miss, regulatory action, leadership exit) AND
  - RSI > 65 OR price below EMA20

  **BEARISH_LONG** (confidence 55-85):
  - Deteriorating fundamentals (margin compression, rising debt, guidance cut) AND
  - Sector headwind confirmed in news

  **INTRADAY_WATCH** (confidence: not applicable, informational only):
  - Any CONFIRMED or VERIFIED_DEEP item with time_horizon SHORT_TERM
  - Fresh within last 6 hours
  - Volume_ratio > 1.2 OR RSI moving sharply
  - Note: this is a watchlist, NOT a trade signal. Requires chart confirmation at open.

- Call Gemini 1.5 Flash to generate the `reasons` field (3 bullets per signal) from the input data
- Write all signals to `state.signals` and to Supabase `signals` table

---

### Node 11: SynthesizerAgent

**Responsibility:** Compile everything into a readable email digest.  
**Reads from state:** `signals`, `tagged_items`, `fii_dii_data`  
**Writes to state:** `digest_html`, `subject_line`, appends to `run_log`  
**Logic:**

- If `signals` is empty AND no CONFIRMED `tagged_items`: set `digest_html = None`, skip to DeliveryAgent
- Call Gemini 1.5 Flash with structured prompt to generate digest sections:
  1. 🚨 Top Signals — BULLISH/BEARISH with confidence, ticker, 3 reasons
  2. 👀 Intraday Watchlist — stocks to monitor next 1-2 sessions with reason
  3. 📊 Sector Overview — which sectors are in motion this session
  4. 📰 Confirmed News — CONFIRMED items without a signal (context only)
  5. ⚠️ Flagged — SINGLE_SOURCE_UNVERIFIED items (read, don't act on)
  6. 📈 Market Flows — FII/DII summary if NSE_CLOSE session
- Generate `subject_line`: "[{session}] {top_signal_ticker} {signal_type} {confidence}% — {sector} in focus"
- Render Jinja2 HTML template with digest content

---

### Node 12: DeliveryAgent

**Responsibility:** Send the email. Log the run. Exit cleanly.  
**Reads from state:** `digest_html`, `subject_line`, `signals`, `email_sent`  
**Writes to state:** `email_sent`, `no_email_reason`, appends to `run_log`  
**Logic:**

- If `digest_html` is None: set `no_email_reason = "no_new_content"`, log, exit
- Else: send via Gmail SMTP using env credentials
- On SMTP error: retry once after 30 seconds, then log error and exit
- Write final run record to Supabase `pipeline_runs` table

---

### Node 13: SupervisorAgent (Sunday only, separate graph)

**Responsibility:** Review weekly accuracy. Propose calibration changes.  
**Reads from:** Supabase — `source_accuracy_log`, `outcome_tracker` (last 7 days), `signals`  
**Writes to:** Supabase `proposed_calibrations`, sends weekly report email  
**Logic:**

- For each source_id: calculate accuracy = correct_outcomes / total_signals_from_source
- For each signal_type: calculate accuracy over last 30 signals
- Propose threshold adjustments in JSON format (does NOT auto-apply — stored in `proposed_calibrations`)
- Generate weekly performance report: what fired, what was right/wrong, what's proposed, why
- Send report via Gmail SMTP with subject "[FINTEL SUPERVISOR] Weekly Report - {date}"

---

## 5. Graph Edges and Conditional Routing

```text
START
  → CollectorAgent
  → DeduplicatorAgent
  → RelevanceFilterAgent
  → NoiseScorerAgent
  → ValidatorAgent
  → [PARALLEL FORK]
      Branch A: FundamentalsAgent → TechnicalAgent → (join)
      Branch B: DeepCheckAgent (only if pending_deep_check is not empty) → (join)
  → TaggerAgent   (waits for both branches)
  → SignalGeneratorAgent
  → SynthesizerAgent
  → DeliveryAgent
  → END

CONDITIONAL EDGES:
  ValidatorAgent → DeepCheckAgent: ONLY if len(state.pending_deep_check) > 0
  ValidatorAgent → TaggerAgent directly: if len(state.pending_deep_check) == 0 (skip DeepCheckAgent)
  SynthesizerAgent → DeliveryAgent: always (DeliveryAgent handles the "nothing to send" case internally)

ERROR HANDLING EDGES:
  Any agent → logs error to state.errors → continues to next node (never hard-abort unless CollectorAgent returns zero items)
  CollectorAgent zero items → log "no_data_collected" → jump directly to DeliveryAgent (no email)
```

---

## 6. What Gets Logged to Supabase (Reference for M6 Schema)

| Table                 | Written by                      | Timing              |
| --------------------- | ------------------------------- | ------------------- |
| pipeline_runs         | DeliveryAgent                   | End of every run    |
| news_items            | DeduplicatorAgent               | Per item, every run |
| fundamentals_cache    | FundamentalsAgent               | Weekly refresh      |
| technical_cache       | TechnicalAgent                  | Every session       |
| signals               | SignalGeneratorAgent            | Per signal          |
| agent_decisions       | ValidatorAgent + DeepCheckAgent | Per decision        |
| source_accuracy_log   | SupervisorAgent                 | Weekly              |
| outcome_tracker       | You (manual during M11)         | After paper trading |
| proposed_calibrations | SupervisorAgent                 | Weekly              |
