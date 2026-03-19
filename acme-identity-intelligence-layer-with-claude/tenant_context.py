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
TENANT CONTEXT — Macy's (Retail)
Enabled features: Stitch, Database, Ingest, Orchestration, Queries, Segments, SparkSQL
NOT enabled: Campaigns, Journeys, Predictive, Bridge, Profile API, Advanced Analytics, BI Connect

EXPANSION OPPORTUNITIES:
- **Campaigns + Journeys** (HIGH priority): Macy's has sophisticated orchestration (100+ orchestrations) but no native campaign/journey capability. Lifecycle marketing — welcome series, win-back, birthday, VIP tier migration — would be immediate value.
- **Predictive** (HIGH priority): With 364M profiles and rich transaction data, predictive CLV, churn propensity, and next-purchase propensity would be transformative. Macy's has the data density to make predictions highly accurate.
- **Bridge** (MEDIUM priority): Currently all activation flows through SFTP. Bridge to Snowflake would modernize their data sharing architecture.
- **Profile API** (FUTURE): Real-time identity for macys.com personalization, store associate clienteling, loyalty app.

USE CASE PRIORITIES:
Crawl: Customer suppression, loyalty tier optimization, cross-channel engagement scoring
Walk: Churn prevention, CLV-based targeting, personalized product recommendations, omnichannel attribution
Run: Real-time web personalization, predictive replenishment, retail media network"""
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
