# Tenant-specific context for AI system prompts
# Maps tenant names to their whitespace data and relevant use cases

TENANT_CONTEXT = {
    "honda": {
        "display_name": "American Honda Motor",
        "vertical": "Automotive",
        "enabled": ["DATABASE", "INGEST", "ORCHESTRATION", "QUERIES", "SEGMENTS", "SPARKSQL", "STITCH"],
        "disabled": ["ADVANCED_ANALYTICS", "BI_CONNECT", "BRIDGE", "CAMPAIGNS", "JOURNEYS", "PREDICTIVE", "PREMIUM_CONNECTORS", "PROFILE_API"],
        "whitespace_prompt": """
TENANT CONTEXT — American Honda Motor (Automotive)
Enabled features: Stitch, Database, Ingest, Orchestration, Queries, Segments, SparkSQL
NOT enabled: Campaigns, Journeys, Predictive, Bridge, Profile API, Advanced Analytics, BI Connect

EXPANSION OPPORTUNITIES (based on whitespace analysis):
- **Campaigns + Journeys** (HIGH priority): Honda has segments and orchestration but no campaign or journey capability. This is the clearest growth path — they can build audiences today but can't activate lifecycle marketing (welcome series, service reminders, lease renewal) natively. Recommend: lifecycle marketing starting with service appointment reminders and lease maturity campaigns.
- **Predictive** (MEDIUM priority): No predictive scoring. Honda would benefit from churn propensity (predict service defection), purchase propensity (next vehicle), and CLV scoring across their owner/prospect base. Maturity: Walk phase.
- **Bridge** (MEDIUM priority): No zero-copy sharing. If Honda uses Snowflake or Databricks for dealer analytics, Bridge eliminates SFTP orchestration overhead.
- **Profile API** (FUTURE): Real-time identity lookup for dealer websites or Honda app personalization. Run phase — requires strong identity foundation first.

USE CASE PRIORITIES (Crawl-Walk-Run):
Crawl (now): Customer suppression from acquisition (stop marketing to existing owners), owner lifecycle segmentation, service retention alerts
Walk (next): Conquest marketing (target competitor owners), dealer-level performance analytics, cross-brand identity (Honda/Acura), lease renewal prediction
Run (future): Real-time dealer website personalization, predictive service scheduling, connected vehicle data integration"""
    },

    "seahawks": {
        "display_name": "Seattle Seahawks",
        "vertical": "Sports & Entertainment",
        "enabled": ["DATABASE", "INGEST", "QUERIES", "SPARKSQL", "STITCH"],
        "disabled": ["ADVANCED_ANALYTICS", "BI_CONNECT", "BRIDGE", "CAMPAIGNS", "JOURNEYS", "ORCHESTRATION", "PREDICTIVE", "PREMIUM_CONNECTORS", "PROFILE_API", "SEGMENTS"],
        "whitespace_prompt": """
TENANT CONTEXT — Seattle Seahawks (Sports & Entertainment)
Enabled features: Stitch, Database, Ingest, Queries, SparkSQL
NOT enabled: Segments, Orchestration, Campaigns, Journeys, Predictive, Bridge, Profile API, Advanced Analytics, BI Connect

CRITICAL NOTE: Seahawks has the most whitespace of any tenant you're analyzing. They have identity resolution but almost no activation capabilities. This is a foundation-only deployment today.

EXPANSION OPPORTUNITIES (based on whitespace analysis):
- **Segments + Orchestration** (CRITICAL priority): Cannot build or activate audiences without these. This is the minimum viable next step. Without segments, the identity data is locked inside the platform.
- **Campaigns** (HIGH priority): Season ticket renewal, single-game buyer conversion, merchandise cross-sell — all require campaign capability.
- **Journeys** (HIGH priority): Fan lifecycle orchestration — from first-game-buyer to season ticket holder to premium seat upgrade. Sports has natural lifecycle moments (season start, playoffs, offseason).
- **Predictive** (MEDIUM priority): Churn prediction for season ticket holders is extremely high value in sports. Predict non-renewal 60-90 days before deadline. Also: propensity to upgrade seat tier.
- **Profile API** (FUTURE): Real-time fan recognition at stadium, app personalization, in-venue offers.

USE CASE PRIORITIES (Crawl-Walk-Run):
Crawl (now): Fan identity unification (ticket + merch + app + email), season ticket holder profiling, duplicate fan record cleanup
Walk (next): Renewal risk scoring, single-game to season-ticket conversion campaigns, merchandise affinity targeting, sponsor activation audiences
Run (future): In-venue real-time recognition, predictive ticket pricing, sponsor ROI measurement, fan lifetime value optimization"""
    },

    "atlas-motors": {
        "display_name": "Atlas Motors",
        "vertical": "Automotive (Demo)",
        "enabled": ["DATABASE", "INGEST", "QUERIES", "SPARKSQL", "STITCH"],
        "disabled": [],
        "whitespace_prompt": """
TENANT CONTEXT — Atlas Motors (Automotive Demo Tenant)
This is a demo/internal tenant for automotive use cases. Standard Amperity schema.

USE CASES TO DEMONSTRATE:
- Identity resolution across Sales, Service, Finance, and Marketing touchpoints
- Owner lifecycle: prospect → buyer → service customer → repeat buyer
- Household-level vehicle ownership intelligence
- Service retention and defection risk
- Conquest marketing: identify competitor owners in your CRM
- Cross-brand identity (if multi-brand dealership group)
- Dealer-level customer analytics"""
    },

    "macys": {
        "display_name": "Macy's",
        "vertical": "Retail",
        "enabled": ["DATABASE", "INGEST", "ORCHESTRATION", "QUERIES", "SEGMENTS", "SPARKSQL", "STITCH"],
        "disabled": ["ADVANCED_ANALYTICS", "BI_CONNECT", "BRIDGE", "CAMPAIGNS", "JOURNEYS", "PREDICTIVE", "PREMIUM_CONNECTORS", "PROFILE_API"],
        "whitespace_prompt": """
TENANT CONTEXT — Macy's (Retail, Enterprise)
Enabled: Stitch, Database, Ingest, Orchestration, Queries, Segments, SparkSQL
NOT enabled: Campaigns, Journeys, Predictive, Bridge, Profile API, Advanced Analytics, BI Connect

OUTPUT RULES — ALL responses must be client-safe:
- Never reference internal strategy, positioning, stakeholder names, or coaching notes
- Never mention vendor names (Deloitte, Acxiom, Epsilon) or internal project codenames
- Never surface stitch scores, escalation details, or implementation partner context
- Frame everything around business outcomes and customer value
- Use "enrichment partner" not vendor names, "implementation partner" not SI names

SCHEMA (CRITICAL — violations destroy credibility):

ID FIELD NAMES (case varies by table — get this right):
- merged_customer, TRANSACTIONS: AMPID (UPPERCASE)
- LOYALTY: AmpID (PascalCase)
- PREFERENCES, CREDIT: ampid (lowercase)
- NEVER use amperity_id — it does not exist in this tenant

CORE TABLES:
- `merged_customer` — 404M rows, 364M distinct AMPIDs. Fields: AMPID, Fname, Lname, Email, Phone, Gender, Add1, City, State, Zip, country, last_txn_date, first_txn_date, household_id, name_priority, address_priority, email_priority, phone_priority
  NO birthdate, NO given_name (use Fname), NO loyalty_tier, NO lifetime_spend, NO email_suppression
- `TRANSACTIONS` — 2B rows, 103M distinct AMPIDs. Fields: AMPID, txn_dt (date), tot_txn_amt (gross), tot_sale_amt (net), tot_disc_amt, tot_rtrn_amt, Datasource, loyal_id_nbr
  NO order_datetime (use txn_dt), NO order_revenue (use tot_txn_amt). Only 2 datasources: sales_txn_hdr_loyalty, sales_txn_hdr_merged_account. Date range: 2019-08-01 to present (~6.7 years)
- `LOYALTY` — 86M distinct AmpIDs. Fields: AmpID, Loyalty_ID, acct_stat_cd, acct_stat_rsn, loyal_enrlmnt_dt_et
  NO loyalty_tier field, NO lifetime_spend field
- `CREDIT` — 46M distinct ampids, 82 columns. Fields: ampid, acct_typ_cd, corp_emp_ind, emp_ind, curr_vip_cd, curr_vip_desc, star_rwd_spnd_lvl_desc
  Suppression flags: suppr_all_advrt_ind, suppr_drct_mail_ind, suppr_email_ind, suppr_telmkt_ind
  Employee detection: corp_emp_ind = 'Y' OR emp_ind = 'Y' (~191K employees)
  GLBA applies — never expose acct_nbr
- `PREFERENCES` — NOT one-row-per-customer. Each row is one preference event. Key fields: ampid, Preftype_Desc, Pref, Channel, Email, Phone, Act_Ind
  Preftype_Desc values: EMAIL_SUBSCRIBE_FLAG (624M rows), DELIVERABLE_STATUS (612M), MOBILE_SUBSCRIBE_FLAG (122M), DO NOT CALL (60M)
  To query suppression: filter by Preftype_Desc and check Pref value. NOT WHERE email_suppression = 'TRUE'
- `REGISTRY` — gift/wedding registry
- `Unified_Coalesced` — stitch output with semantic tags. Use only for stitch debugging
- `Merged_Households` — household groupings (address + surname SHA hash)
- `Customer_Master` — extended attributes: employee_flag, dob, birthday_day/month/year, loyal_tier_cd, loyalty_id, hhld_incm_cd

TABLES THAT DO NOT EXIST (never reference):
- Unified_Transactions — use TRANSACTIONS
- loyal_acct_stitch — use LOYALTY
- Merged_Customers (capital C, plural S) — use merged_customer
- Customer_360 — customer_360 flag is false

SQL PATTERNS (always use these):
- Time: ALWAYS relative (CURRENT_DATE - INTERVAL '180' DAY), never hardcoded dates
- Employee exclusion: JOIN CREDIT c ON mc.AMPID = c.ampid WHERE (c.corp_emp_ind = 'Y' OR c.emp_ind = 'Y') — or via Customer_Master employee_flag
- Email exclusions: AND LOWER(mc.Email) NOT LIKE '%macys%' AND LOWER(mc.Email) NOT LIKE '%bloomingdales%' AND LOWER(mc.Email) NOT LIKE '%borderx%'
- Test exclusions: AND LOWER(mc.Lname) NOT LIKE '%test%' AND LOWER(mc.Lname) NOT LIKE '%egc%'
- Suppression via PREFERENCES: SELECT DISTINCT ampid FROM PREFERENCES WHERE Preftype_Desc = 'EMAIL_SUBSCRIBE_FLAG' AND Pref = 'N'
- Suppression via CREDIT: WHERE suppr_email_ind = 'Y' OR suppr_all_advrt_ind = 'Y'
- VIP tiers: JOIN CREDIT for star_rwd_spnd_lvl_desc, curr_vip_desc
- Cross-system: SELECT AMPID FROM TRANSACTIONS GROUP BY AMPID HAVING COUNT(DISTINCT Datasource) > 1

AUDIENCE SIZES (verified March 19, 2026 — use ONLY these, never estimate):
- Total profiles (distinct AMPIDs): 364M
- Transacting customers: 103M
- Has email: 276M (76% of profiles)
- Has phone: 99M (27%)
- Has both email + phone: 74.5M (20%)
- Email-only (no phone): ~201M
- Transacting + has email: 88M
- Loyalty members: 86M
- Credit card holders: 46M
- Employees to exclude: 191K
- Cross-system customers (2+ datasources): 33.5M (32.6% of transacting)
- One-purchase customers: 25.7M at $111.51 avg
- Transaction rows: 2B across ~6.7 years
- Win-back 180-day dormant: 25-30M (estimated from transacting base)
- Deduplication rate: ~69% (404M rows to 364M distinct)
- Stitch singletons: 185M

PLATFORM STATE (what works, what's blocked):
WORKS NOW: CDA analytics queries (counts, spend, churn, visualizations), SQL in query editor, 100+ Snowflake orchestrations, stitch review/debugging
BLOCKED (requires Phase 1 — semantic tags + customer_360 flag): CDA segment creation, journey creation, visual segment editor, AmpIQ suite
REQUIRES ADDITIONAL PRODUCTS (do not assume available): Profile API, Campaigns + Journeys native, Predictive models

EXPANSION OPPORTUNITIES:
- Semantic tagging sprint (CRITICAL next step): Unlocks CDA segmentation, journey builder, AmpIQ immediately. Single highest-impact action.
- Campaigns + Journeys (HIGH, post-tagging): 100+ orchestrations prove activation need. Native campaigns 3-5x faster.
- Predictive (HIGH, post-tagging): Data density supports CLV, churn, propensity scoring. Proactive retention.
- Bridge (MEDIUM): Replace SFTP orchestrations with zero-copy Snowflake sharing.
- Profile API (FUTURE): macys.com personalization, store clienteling, loyalty app.

USE CASE PRIORITIES:
Crawl (now): Customer suppression, loyalty profiling, basic segmentation, CDA analytics
Walk (post-Phase 1): Churn prevention, CLV targeting, omnichannel attribution, lifecycle campaigns, self-serve audiences
Run (future): Real-time personalization, predictive replenishment, retail media network, journey orchestration

KEY METRICS FOR RECOMMENDATIONS:
- Win-back: 25.7M one-and-done at $112 avg. At 5% reactivation = ~$144M recoverable
- Cross-system resolution: 33.5M customers across multiple systems = identity resolution proving value
- Email gap: 76% have email but only 27% have phone = enrichment opportunity
- Loyalty penetration: 86M of 364M = 24% loyalty membership rate

NEVER SAY (violations destroy credibility):
- C360 is "complete" or "done" — customer_360 flag is false
- "396M customers with email" — that's source records pre-dedup; actual is 276M (76%)
- "165M active customers" — number does not exist; use 364M profiles or 103M transacting
- "112M loyalty members" — actual is 86M
- "1.22B source records" — actual transaction rows are 2B
- "10 source systems" for transactions — there are 2 datasources
- "~100% email coverage" — actual is 76%
- Hardcoded dates in SQL
- amperity_id, order_datetime, order_revenue, given_name, birthdate — none exist
- Unified_Transactions — does not exist
- Call stitch "probabilistic" — it is hybrid (exact-match + fuzzy + AI scoring)
- Quote Amps pricing or estimate product costs
- Reference internal stakeholder names, vendor names, SI partners, or strategy notes"""
    },

    "gm": {
        "display_name": "General Motors",
        "vertical": "Automotive",
        "enabled": ["BRIDGE", "DATABASE", "INGEST", "ORCHESTRATION", "QUERIES", "SEGMENTS", "SPARKSQL", "STITCH"],
        "disabled": ["ADVANCED_ANALYTICS", "BI_CONNECT", "CAMPAIGNS", "JOURNEYS", "PREDICTIVE", "PREMIUM_CONNECTORS", "PROFILE_API"],
        "whitespace_prompt": """
TENANT CONTEXT — General Motors (Automotive, Multi-Region: US + MEO + Mexico)
Enabled features: Stitch, Database, Ingest, Orchestration, Queries, Segments, SparkSQL, Bridge
NOT enabled: Campaigns, Journeys, Predictive, Profile API, Advanced Analytics

GM is unique: three separate tenants (US, MEO, Mexico) with no native cross-tenant intelligence. The Intelligence Layer bridges this gap.

EXPANSION OPPORTUNITIES:
- **Campaigns + Journeys** (HIGH priority): GM has segments and orchestration but can't run lifecycle campaigns natively. Service retention, lease renewal, model launch campaigns.
- **Predictive** (HIGH priority): Vehicle purchase cycle prediction, service defection risk, lease vs buy propensity. Automotive has strong signal data.
- **Cross-tenant intelligence** (UNIQUE to this layer): Comparing identity quality across US/MEO/Mexico, standardizing approaches, identifying shared customers across markets.
- **Profile API** (FUTURE): Dealer website real-time recognition, connected vehicle personalization.

USE CASE PRIORITIES:
Crawl: Owner suppression from conquest campaigns, service retention segmentation, regional identity quality benchmarking
Walk: Lease renewal prediction, cross-brand identity (Chevy/GMC/Buick/Cadillac), dealer performance analytics
Run: Connected vehicle data integration, real-time dealer personalization, global customer identity"""
    },

    "nfl": {
        "display_name": "NFL",
        "vertical": "Sports & Entertainment",
        "enabled": ["DATABASE", "INGEST", "QUERIES", "SPARKSQL", "STITCH"],
        "disabled": ["ADVANCED_ANALYTICS", "BI_CONNECT", "BRIDGE", "CAMPAIGNS", "JOURNEYS", "ORCHESTRATION", "PREDICTIVE", "PREMIUM_CONNECTORS", "PROFILE_API", "SEGMENTS"],
        "whitespace_prompt": """
TENANT CONTEXT — NFL (Sports & Entertainment, League-Level)
Enabled features: Stitch, Database, Ingest, Queries, SparkSQL
NOT enabled: Segments, Orchestration, Campaigns, Journeys, Predictive, Bridge, Profile API, Advanced Analytics

CRITICAL NOTE: NFL is a league-level deployment — identity resolution across 32 clubs, league office, and multiple fan touchpoints. Similar whitespace gap as Seahawks but at massive scale.

EXPANSION OPPORTUNITIES:
- **Segments + Orchestration** (CRITICAL): Same as Seahawks — cannot build or activate audiences without these. League-wide fan segmentation is the minimum next step.
- **Campaigns + Journeys** (HIGH priority): League-level fan engagement — Super Bowl, Draft, season kickoff campaigns. Cross-club fan journey orchestration.
- **Bridge** (HIGH priority): Club-level data sharing via Snowflake/Databricks. Enable clubs to access league-level unified fan profiles without direct tenant access.
- **Predictive** (MEDIUM priority): Fan engagement scoring, ticket purchase propensity, merchandise affinity. League-wide churn prediction for NFL+ subscribers.
- **Profile API** (FUTURE): Real-time fan recognition across NFL.com, NFL app, stadium venues.

USE CASE PRIORITIES:
Crawl: League-wide fan identity unification, cross-club duplicate resolution, data quality baseline across all 32 clubs
Walk: NFL+ subscriber retention, cross-club fan migration tracking, Super Bowl audience activation, sponsor audience delivery
Run: Real-time stadium personalization, predictive ticket pricing, cross-platform fan journey orchestration, media rights audience verification"""
    },

    "acme": {
        "display_name": "Acme",
        "vertical": "Demo/Internal",
        "enabled": [],
        "disabled": [],
        "whitespace_prompt": """
TENANT CONTEXT — Acme (Demo Tenant)
This is Amperity's internal demo tenant with synthetic data across multiple sources (eCommerce, POS, Loyalty, Wifi, Web). Standard Amperity schema.

Good for demonstrating all identity resolution capabilities without client data concerns."""
    }
}


def get_tenant_context(tenant_name: str) -> str:
    """Get the whitespace/use-case context for a tenant."""
    key = tenant_name.lower().replace(" ", "").replace("-", "").replace("_", "")

    # Try exact and fuzzy matches
    for k, v in TENANT_CONTEXT.items():
        if k.replace("-", "").replace("_", "") == key:
            return v.get("whitespace_prompt", "")

    # Try partial match
    for k, v in TENANT_CONTEXT.items():
        if k in key or key in k:
            return v.get("whitespace_prompt", "")

    return ""
