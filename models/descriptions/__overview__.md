{% docs __overview__ %}

# Welcome to the Flipside Crypto Osmosis Models Documentation

## **What does this documentation cover?**
The documentation included here details the design of the Osmosis
 tables and views available via [Flipside Crypto.](https://flipsidecrypto.xyz/) For more information on how these models are built, please see [the github repository.](https://github.com/flipsideCrypto/osmosis-models/)

## **How do I use these docs?**
The easiest way to navigate this documentation is to use the Quick Links below. These links will take you to the documentation for each table, which contains a description, a list of the columns, and other helpful information.

If you are experienced with dbt docs, feel free to use the sidebar to navigate the documentation, as well as explore the relationships between tables and the logic building them.

There is more information on how to use dbt docs in the last section of this document.

## **Quick Links to Table Documentation**

**Click on the links below to jump to the documentation for each schema.**

### Core Tables (`OSMOSIS`.`CORE`.`<table_name>`)
### DeFi Tables (`OSMOSIS`.`DEFI`.`<table_name>`)
### Governance Tables (`OSMOSIS`.`GOV`.`<table_name>`)
### Prices Tables (`OSMOSIS`.`PRICE`.`<table_name>`)
### Statistics/Analytics Tables (`OSMOSIS`.`STATS`.`<table_name>`)



**Core Dimension Tables:**
- [dim_labels](#!/model/model.osmosis_models.core__dim_labels)
- [dim_tokens](#!/model/model.osmosis_models.core__dim_tokens)

**Core Fact Tables:**
- [fact_airdrop](#!/model/model.osmosis_models.core__fact_airdrop)
- [fact_blocks](#!/model/model.osmosis_models.core__fact_blocks)
- [fact_daily_balances](#!/model/model.osmosis_models.core__fact_daily_balances)
- [fact_msg_attributes](#!/model/model.osmosis_models.core__fact_msg_attributes)
- [fact_msgs](#!/model/model.osmosis_models.core__fact_msgs)
- [fact_token_day](#!/model/model.osmosis_models.core__fact_token_day)
- [fact_transactions](#!/model/model.osmosis_models.core__fact_transactions)
- [fact_transfers](#!/model/model.osmosis_models.core__fact_transfers)

**Core Convenience Tables:**
- [ez_icns](#!/model/model.osmosis_models.core__ez_icns)

**DeFi Dimension Tables:**
- [dim_liquidity_pools](#!/model/model.osmosis_models.defi__dim_liquidity_pools)

**DeFi Fact Tables:**
- [fact_liquidity_provider_actions](#!/model/model.osmosis_models.defi__fact_liquidity_provider_actions)
- [fact_locked_liquidity_actions](#!/model/model.osmosis_models.defi__fact_locked_liquidity_actions)
- [fact_pool_fee_day](#!/model/model.osmosis_models.defi__fact_pool_fee_day)
- [fact_pool_hour](#!/model/model.osmosis_models.defi__fact_pool_hour)
- [fact_superfluid_staking](#!/model/model.osmosis_models.defi__fact_superfluid_staking)
- [fact_swaps](#!/model/model.osmosis_models.defi__fact_swaps)

**Governance Dimension Tables:**
- [dim_dim_vote_options](#!/model/model.osmosis_models.core__dim_vote_options)

**Governance Fact Tables:**
- [fact_governance_proposal_deposits](#!/model/model.osmosis_models.gov__fact_governance_proposal_deposits)
- [fact_governance_submit_proposal](#!/model/model.osmosis_models.gov__fact_governance_submit_proposal)
- [fact_governance_validator_votes](#!/model/model.osmosis_models.gov__fact_governance_validator_votes)
- [fact_governance_votes](#!/model/model.osmosis_models.cgov__fact_governance_votes)
- [fact_staking](#!/model/model.osmosis_models.cgov__fact_staking)
- [fact_staking_rewards](#!/model/model.osmosis_models.gov__fact_staking_rewards)
- [fact_validators](#!/model/model.osmosis_models.gov__fact_validators)


**Prices FacDimensiont Tables:**
- [dim_prices ](#!/model/model.osmosis_models.core__dim_prices)

**Prices Convenience Tables:**
- [ez_prices](#!/model/model.osmosis_models.core__ez_prices)

**Stats EZ Tables:**
- [ez_core_metrics_hourly](#!/model/model.osmosis_models.ez_core_metrics_hourly)


## **Data Model Overview**

The Osmosis models are built a few different ways, but the core fact tables are built using three layers of sql models: **bronze, silver, and gold (or core).**

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (core/defi/gov/price): Final views and tables that are available publicly

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted ez_) are a combination of different fact and dimension tables. These views are built to make it easier to query the data.

## **Using dbt docs**
### Navigation

You can use the ```Project``` and ```Database``` navigation tabs on the left side of the window to explore the models in the project.

### Database Tab

This view shows relations (tables and views) grouped into database schemas. Note that ephemeral models are *not* shown in this interface, as they do not exist in the database.

### Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models.

On model pages, you'll see the immediate parents and children of the model you're exploring. By clicking the Expand button at the top-right of this lineage pane, you'll be able to see all of the models that are used to build, or are built from, the model you're exploring.

Once expanded, you'll be able to use the ```--models``` and ```--exclude``` model selection syntax to filter the models in the graph. For more information on model selection, check out the [dbt docs](https://docs.getdbt.com/docs/model-selection-syntax).

Note that you can also right-click on models to interactively filter and explore the graph.


### **More information**
- [Flipside](https://flipsidecrypto.xyz/)
- [Data Studio](https://flipsidecrypto.xyz/edit)
- [Tutorials](https://docs.flipsidecrypto.com/our-data/tutorials)
- [Github](https://github.com/FlipsideCrypto/osmosis-models)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)

{% enddocs %}