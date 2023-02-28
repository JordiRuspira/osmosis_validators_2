#!/usr/bin/env python
# coding: utf-8

# In[1]:

import datetime
import streamlit as st
import pandas as pd
import requests
import json
import time
import plotly.graph_objects as go
import random
import plotly.io as pio
import streamlit as st
import pandas as pd
import numpy as np
from shroomdk import ShroomDK
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as md
import matplotlib.ticker as ticker
import numpy as np
import plotly.express as px
sdk = ShroomDK("3b5afbf4-3004-433c-9b04-2e867026718b")
st.cache(suppress_st_warning=True)  
st.set_page_config(page_title=":atom_symbol: Osmosis Governance :atom_symbol:", layout="wide",initial_sidebar_state="collapsed")

# In[2]:
st.title('Osmosis Governance')

# In[3]:
st.markdown('Governance and validator power is a recurrent topic when talking about delegated proof of stake blockchains. This dashboard and tool pretends show how users behave with respect to the validators they are delegating to, as well as how governance would be without a delegated PoS system, just a proof of stake one. Therefore, this dashboard should help validators have a better understanding and insights of how their voting options impacts on their delegators.')
st.markdown('This dashboard is structured as follows:')
st.write('- First off, you should select a proposal ID to inspect how that specific proposal went.')
st.write('- You then are presented with three tabs. The first one is just an overview of how individual validators voted on that proposal,how many delegators voted and the turnout it would have reached if only their vote counted (over total percentage of Osmo staked). It also shows the turnout for all proposals already voted. Spoiler: almost no proposal would have reached quorum if it werent for the delegated proof of stake system.')
st.write('- After this section come the insights for specific validators in a second tab. Once a proposal ID has been selected in the previous section, you should choose a validator from the select box to see how delegators reacted for that validator and proposal. The numbers displayed here show movement between the go live of the proposal and 7 days after the ending of the vote. Again, there are multiple tabs to show different information.')
st.write('- The third and last section allows you to select a validator and a date, and shows a sankey chart with all redelegations from that validator to other validators.')
st.markdown('If there`s any missing information or misleading one, please do not hesitate to reach out on twitter.')

 

# In[4]:
sql0 = f"""
with votes_times as 
(select proposal_id, max(date_trunc('day', block_timestamp)) as date 
 from osmosis.core.fact_governance_votes
 where tx_succeeded = 'TRUE' 
group by proposal_id ),  
validators_address as (
    select address, label, rank, raw_metadata:"account_address" as account_address
    from osmosis.core.fact_validators 
    ),
val_votes_aux as 
(
 select voter, 
 proposal_id, 
 b.description, 
 rank() over (partition by voter, proposal_id order by block_timestamp desc) as rank
 from osmosis.core.fact_governance_votes a 
 left join osmosis.core.dim_vote_options b 
 on a.vote_option = b.vote_id
 where voter in (select distinct account_address from validators_address) 
 and tx_succeeded = 'TRUE'
 )
 
select voter, b.address, proposal_id, description, label, b.rank, 1 as value from val_votes_aux a
left join validators_address b 
on a.voter = b.account_address
where a.rank = 1 
and b.rank <= 150
"""

sql1 = """
select distinct proposal_id as proposal_id
from osmosis.core.fact_governance_votes
where tx_succeeded = 'TRUE' 
"""

sql2 = """
select address, account_address, label, rank from  osmosis.core.fact_validators
where rank <= 150

"""

# In[5]:


st.experimental_memo(ttl=1000000)
@st.experimental_memo
def compute(a):
    results=sdk.query(a)
    return results

results0 = compute(sql0)
df0 = pd.DataFrame(results0.records)
df0.info()

results1 = compute(sql1)
df1 = pd.DataFrame(results1.records)
df1.info()

results2 = compute(sql2)
df2 = pd.DataFrame(results2.records)
df2.info()


# In[6]:

import math

st.write('')
st.header('Selecting a proposal ID') 
st.write('The first step in the dashboard would be selecting/typing a proposal ID using the selectbox below. Afterwards, the pages below will also refresh showing:') 
st.write('- Validator vote, total delegator votes and historical turnout')
st.write('- In a second tab, you can navigate to inspect how the proposal affected to redelegations from/to a sepecific delegator, as well as some additional info.')
selection = df1['proposal_id'].unique()
selection_sorted = selection.sort()
proposal_choice = '427'
proposal_choice = st.selectbox("Select a proposal", options = selection ) 
 
 
st.write('Keep in mind that the following data regarding total osmo staked, I`ve not taken into account superfluid staked OSMO, since it cannot be overwritten by delegators. It currently sits around 30M OSMO staked, so the real turnout if only delegators voted would be even lower.')

df0_fil = df0[df0['proposal_id'] == proposal_choice]

tab1, tab2, tab3 = st.tabs(["Validator vote, delegator vote and historical turnout", "Inspect a validator", "Sankey chart for redelegations"])

# In[7]:

with tab1:
    
    
    st.subheader("Validator vote for the selected proposal")
    st.write('')
    st.write('The following chart shows how validators voted for the selected proposal.')
    st.write('')
    fig1 = px.bar(df0_fil, x="label", y="value", color="description", color_discrete_sequence=px.colors.qualitative.Pastel2)
    fig1.update_layout(
    title='Validator voting choice for selected proposal',
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1 # gap between bars of the same location coordinate.
    )
    st.plotly_chart(fig1, theme="streamlit", use_container_width=True)
 
    df_query_aux2 ="""
   with votes_times as 
(select proposal_id, max(date_trunc('day', block_timestamp)) as date 
 from osmosis.core.fact_governance_votes
 where tx_succeeded = 'TRUE'
 and proposal_id =  '"""

    sql6 = df_query_aux2 + str(proposal_choice) + """'
group by proposal_id ),  
   
    
votes_proposal_aux as (
select voter, proposal_id, vote_option, rank() over (partition by voter, proposal_id order by block_timestamp desc) as rank
from osmosis.core.fact_governance_votes
where tx_succeeded = 'TRUE'
and proposal_id in (select proposal_id from votes_times)
), 
votes_proposal as 
(select voter, 
proposal_id,
b.description
from votes_proposal_aux a 
left join osmosis.core.dim_vote_options b 
on a.vote_option = b.vote_id
where a.rank = 1
and proposal_id in (select proposal_id from votes_times)
),
delegations as (
select date_trunc('day', block_timestamp) as date,
delegator_address,
validator_address,
sum(amount/pow(10, decimal)) as amount 
from osmosis.core.fact_staking
where tx_succeeded = 'TRUE' 
and action = 'delegate'
and date_trunc('day', block_timestamp) <= (select date from votes_times)
group by date, delegator_address, validator_address
),
undelegations as (
select date_trunc('day', block_timestamp) as date,
delegator_address,
validator_address,
sum(amount/pow(10, decimal))*(-1) as amount 
from osmosis.core.fact_staking
where tx_succeeded = 'TRUE' 
and action = 'undelegate'
and date_trunc('day', block_timestamp) <= (select date from votes_times)
group by date, delegator_address, validator_address
),
redelegations_to as 
(
select date_trunc('day', block_timestamp) as date,
delegator_address,
validator_address,
sum(amount/pow(10, decimal)) as amount 
from osmosis.core.fact_staking
where tx_succeeded = 'TRUE' 
and action = 'redelegate'
and date_trunc('day', block_timestamp) <= (select date from votes_times)
group by date, delegator_address, validator_address
),
redelegations_from as 
(
select date_trunc('day', block_timestamp) as date,
delegator_address,
redelegate_source_validator_address as validator_address,
sum(amount/pow(10, decimal))*(-1) as amount 
from osmosis.core.fact_staking
where tx_succeeded = 'TRUE' 
and action = 'redelegate'
and date_trunc('day', block_timestamp) <= (select date from votes_times)
group by date, delegator_address, redelegate_source_validator_address
),

total_amount_staked as (
select    
sum(amount) as total_amount
from (
  select * from delegations
  union all 
  select * from undelegations 
  union all 
  select * from redelegations_to 
  union all 
  select * from redelegations_from
  ) a 
),

total_amount_staked_voters as (
select delegator_address, description,
sum(amount) as total_amount
from (
  select * from delegations
  union all 
  select * from undelegations 
  union all 
  select * from redelegations_to 
  union all 
  select * from redelegations_from
  ) a 
left join votes_proposal b 
on a.delegator_address = b.voter
group by delegator_address, description
)

select case when description is null then 'Did not vote'
else 'Voted' end as casuistic, 
count(distinct delegator_address) as num_addresses,
sum(a.total_amount) as total_amount_group,
b.total_amount, 
(total_amount_group/b.total_amount)*100 as percentage
from total_amount_staked_voters a 
join total_amount_staked b 
group by casuistic, b.total_amount
   """
   
    st.experimental_memo(ttl=1000000)
    @st.experimental_memo
    def compute_1(a):
        results=sdk.query(a)
        return results
      
    results6 = compute_1(sql6)
    df6 = pd.DataFrame(results6.records)
    
    st.subheader("Turnout for the selected proposal")
    st.write('')
    st.write('Next, we can see how delegators voted. Please note that when calculating the percentage over total staked Osmo, I have not taken into account superfluid staked Osmo. This turns out to be even worse for delegator turnout since SFS Osmo cannot yet be overwritten.')
    col1, col2 = st.columns(2) 
    
    fig1 = px.bar(df6, x="casuistic", y="percentage", color_discrete_sequence=px.colors.qualitative.Pastel2)
    fig1.update_layout(
    title="Selected proposal - delegator total percentage",
    xaxis_title="Vote/did not vote",
    yaxis_title="Percentage of total staked at the time", 
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1 # gap between bars of the same location coordinate.
    )
    col1.plotly_chart(fig1, theme="streamlit", use_container_width=True)
   
    fig1 = px.bar(df6, x="casuistic", y="total_amount_group", color_discrete_sequence=px.colors.qualitative.Pastel2)
    fig1.update_layout(
    title="Selected proposal - delegator votes",
    xaxis_title="Vote/did not vote",
    yaxis_title="Amount (OSMO) staked", 
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1 # gap between bars of the same location coordinate.
    )
    col2.plotly_chart(fig1, theme="streamlit", use_container_width=True)   
    
    
    sql_det = df_query_aux2 + str(proposal_choice) + """'
group by proposal_id ),  
   
    
votes_proposal_aux as (
select voter, proposal_id, vote_option, rank() over (partition by voter, proposal_id order by block_timestamp desc) as rank
from osmosis.core.fact_governance_votes
where tx_succeeded = 'TRUE'
and proposal_id in (select proposal_id from votes_times)
), 
votes_proposal as 
(select voter, 
proposal_id,
b.description
from votes_proposal_aux a 
left join osmosis.core.dim_vote_options b 
on a.vote_option = b.vote_id
where a.rank = 1
and proposal_id in (select proposal_id from votes_times)
),
delegations as (
select date_trunc('day', block_timestamp) as date,
delegator_address,
validator_address,
sum(amount/pow(10, decimal)) as amount 
from osmosis.core.fact_staking
where tx_succeeded = 'TRUE' 
and action = 'delegate'
and date_trunc('day', block_timestamp) <= (select date from votes_times)
group by date, delegator_address, validator_address
),
undelegations as (
select date_trunc('day', block_timestamp) as date,
delegator_address,
validator_address,
sum(amount/pow(10, decimal))*(-1) as amount 
from osmosis.core.fact_staking
where tx_succeeded = 'TRUE' 
and action = 'undelegate'
and date_trunc('day', block_timestamp) <= (select date from votes_times)
group by date, delegator_address, validator_address
),
redelegations_to as 
(
select date_trunc('day', block_timestamp) as date,
delegator_address,
validator_address,
sum(amount/pow(10, decimal)) as amount 
from osmosis.core.fact_staking
where tx_succeeded = 'TRUE' 
and action = 'redelegate'
and date_trunc('day', block_timestamp) <= (select date from votes_times)
group by date, delegator_address, validator_address
),
redelegations_from as 
(
select date_trunc('day', block_timestamp) as date,
delegator_address,
redelegate_source_validator_address as validator_address,
sum(amount/pow(10, decimal))*(-1) as amount 
from osmosis.core.fact_staking
where tx_succeeded = 'TRUE' 
and action = 'redelegate'
and date_trunc('day', block_timestamp) <= (select date from votes_times)
group by date, delegator_address, redelegate_source_validator_address
),

total_amount_staked as (
select    
sum(amount) as total_amount
from (
  select * from delegations
  union all 
  select * from undelegations 
  union all 
  select * from redelegations_to 
  union all 
  select * from redelegations_from
  ) a 
),

total_amount_staked_voters as (
select delegator_address, description,
sum(amount) as total_amount
from (
  select * from delegations
  union all 
  select * from undelegations 
  union all 
  select * from redelegations_to 
  union all 
  select * from redelegations_from
  ) a 
left join votes_proposal b 
on a.delegator_address = b.voter
group by delegator_address, description
)

select description, 
count(distinct delegator_address) as num_addresses,
sum(a.total_amount) as total_amount_group 
from total_amount_staked_voters a
where description is not null
group by description
   """
   
    st.experimental_memo(ttl=1000000)
    @st.experimental_memo
    def compute_10(a):
        results=sdk.query(a)
        return results
      
    results_det = compute_1(sql_det)
    df_det = pd.DataFrame(results_det.records)
    
    st.write('')
    st.write('The following charts show, out of all delegators who voted, their voting distribution by staked amount.')
    st.write('')
    
    col1, col2 = st.columns(2) 
    
    fig1 = px.bar(df_det, x="description", y="total_amount_group", color_discrete_sequence=px.colors.qualitative.Pastel2)
    fig1.update_layout(
    title="Selected proposal - delegator votes by choice",
    xaxis_title="Voting choice",
    yaxis_title="Total amount staked by delegators", 
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1 # gap between bars of the same location coordinate.
    )
    col1.plotly_chart(fig1, theme="streamlit", use_container_width=True)
   
    fig1 = px.bar(df_det, x="description", y="num_addresses", color_discrete_sequence=px.colors.qualitative.Pastel2)
    fig1.update_layout(
    title="Selected proposal - number of delegators by choice",
    xaxis_title="Voting choice",
    yaxis_title="Number of delegators", 
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1 # gap between bars of the same location coordinate.
    )
    col2.plotly_chart(fig1, theme="streamlit", use_container_width=True)   
    
# In[8]:
    
    st.subheader("Historical turnout")
    st.write('')
    st.write('Finally, using the same criteria, we can look at historical turnout for previous proposals. Note that almost no proposals would have reached the 20% quorum needed for it to pass if it wasn`t for the validator votes.')
    st.write('')
    df_allvotes = pd.read_csv('allvotes.csv')
    df_allvotes_filtered = df_allvotes[df_allvotes['casuistic'] == 'Voted']
    df_allvotes_filtered = df_allvotes_filtered.sort_values(by ='proposal_id', ascending = True)

    fig1 = px.bar(df_allvotes_filtered, x="proposal_id", y="percentage", color_discrete_sequence=px.colors.qualitative.Pastel2)
    fig1.update_layout(
    title="Historical turnout if only delegators voted",
    xaxis_title="Proposal ID",
    yaxis_title="Percentage over total amount staked", 
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14 ,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1 # gap between bars of the same location coordinate.
    )
    fig1.add_shape(type='line',x0=0,
                y0=20,
                x1=450,
                y1=20,
                line=dict(color='Red',),
                xref='x',
                yref='y'
     )
    st.plotly_chart(fig1, theme="streamlit", use_container_width=True)  
    
    
# In[8]:
    
import plotly.graph_objects as go
from plotly.subplots import make_subplots

with tab2:
    
    st.subheader('Selecting a validator')
    st.write('')
    st.write('Now that you have already selected a proposal, it`s time to select a validator too, and this tab will refresh with that choice.')
    st.write('Use the selectbox below to select the validator you want to analyze.') 
    validator_choice = 'Stakecito'
    validator_choice = st.selectbox("Select a validator", options = df2['label'].unique() ) 
    
    # In[9]:
    
    
           
    
      
    df_query_aux2 ="""
    with votes_times as 
    (select proposal_id, max(date_trunc('day', block_timestamp)) as date 
     from osmosis.core.fact_governance_votes
     where tx_succeeded = 'TRUE'
     and proposal_id =  '"""
    
    sql3 = df_query_aux2 + str(proposal_choice) + """' 
    group by proposal_id ),  
       
        
    votes_proposal_aux as (
    select voter, proposal_id, vote_option, rank() over (partition by voter, proposal_id order by block_timestamp desc) as rank
    from osmosis.core.fact_governance_votes
    where tx_succeeded = 'TRUE'
    and proposal_id =  '""" + str(proposal_choice) + """'
    ),
    votes_proposal as 
    (select voter, 
    proposal_id,
    b.description
    from votes_proposal_aux a 
    left join osmosis.core.dim_vote_options b 
    on a.vote_option = b.vote_id
    where a.rank = 1
    and proposal_id =  '""" + str(proposal_choice) + """'
    ),
    delegations as (
    select date_trunc('day', block_timestamp) as date,
    delegator_address,
    validator_address,
    sum(amount/pow(10, decimal)) as amount 
    from osmosis.core.fact_staking
    where tx_succeeded = 'TRUE' 
    and action = 'delegate'
    and date_trunc('day', block_timestamp) <= (select date from votes_times)
    group by date, delegator_address, validator_address
    ),
    undelegations as (
    select date_trunc('day', block_timestamp) as date,
    delegator_address,
    validator_address,
    sum(amount/pow(10, decimal))*(-1) as amount 
    from osmosis.core.fact_staking
    where tx_succeeded = 'TRUE' 
    and action = 'undelegate'
    and date_trunc('day', block_timestamp) <= (select date from votes_times)
    group by date, delegator_address, validator_address
    ),
    redelegations_to as 
    (
    select date_trunc('day', block_timestamp) as date,
    delegator_address,
    validator_address,
    sum(amount/pow(10, decimal)) as amount 
    from osmosis.core.fact_staking
    where tx_succeeded = 'TRUE' 
    and action = 'redelegate'
    and date_trunc('day', block_timestamp) <= (select date from votes_times)
    group by date, delegator_address, validator_address
    ),
    redelegations_from as 
    (
    select date_trunc('day', block_timestamp) as date,
    delegator_address,
    redelegate_source_validator_address as validator_address,
    sum(amount/pow(10, decimal))*(-1) as amount 
    from osmosis.core.fact_staking
    where tx_succeeded = 'TRUE' 
    and action = 'redelegate'
    and date_trunc('day', block_timestamp) <= (select date from votes_times)
    group by date, delegator_address, redelegate_source_validator_address
    ),
    validators_address as (
    select address, label, raw_metadata:"account_address" as account_address
    from osmosis.core.fact_validators 
    ),
    val_votes_aux as 
    (
    select voter, 
    proposal_id, 
    b.description, 
    rank() over (partition by voter, proposal_id order by block_timestamp desc) as rank
    from osmosis.core.fact_governance_votes a 
    left join osmosis.core.dim_vote_options b 
    on a.vote_option = b.vote_id
    where voter in (select distinct account_address from validators_address)
    and proposal_id =  '""" + str(proposal_choice) +"""' 
    and tx_succeeded = 'TRUE'
    ),
    val_votes as (
    select voter, b.address, proposal_id, description from val_votes_aux a
    left join validators_address b 
    on a.voter = b.account_address
    where rank = 1 
    ),
    all_votes_per_proposal_and_validator as 
    (
    select 
    delegator_address, 
    case when b.voter is null then 'Did not vote'
    else b.description end as vote, 
    validator_address, 
    c.label, 
    c.rank,
    case when d.description is null then 'Did not vote'
    else d.description end as validator_vote,
    sum(amount) as total_amount
    from (
      select * from delegations
      union all 
      select * from undelegations 
      union all 
      select * from redelegations_to 
      union all 
      select * from redelegations_from
      ) a 
    left join votes_proposal b 
    on a.delegator_address = b.voter
    left join osmosis.core.fact_validators c 
    on a.validator_address = c.address 
    left join val_votes d 
    on a.validator_address = d.address 
    where c.label =  '""" + str(validator_choice) + """'
    group by 
    delegator_address, 
    vote,
    validator_address,
    c.label,
    c.rank,
    validator_vote
    )
    select validator_vote,
    vote as delegator_vote,
    count(distinct delegator_address) as num_voters,
    sum(total_amount) as total_amount 
    from all_votes_per_proposal_and_validator 
    group by validator_vote,
    vote
    """
 
 # In[13]:

    sql_val_det = df_query_aux2 + str(proposal_choice) +"""'
    group by proposal_id
    ),
    -- This part is done to drop later all duplicate votes
    votes_proposal_aux as (
    select voter, proposal_id, vote_option, rank() over (partition by voter, proposal_id order by block_timestamp desc) as rank
    from osmosis.core.fact_governance_votes
    where tx_succeeded = 'TRUE'
    and proposal_id =  '"""+ str(proposal_choice)+"""'
    ),
    votes_proposal as 
    (select voter, 
    proposal_id,
    b.description
    from votes_proposal_aux a 
    left join osmosis.core.dim_vote_options b 
    on a.vote_option = b.vote_id
    where a.rank = 1
    and proposal_id =  '"""+str(proposal_choice) +"""'
    ),
    redelegations as 
    (
    select date_trunc('day', block_timestamp) as date,
    delegator_address,
    validator_address,
    redelegate_source_validator_address,
    sum(amount/pow(10, decimal)) as amount 
    from osmosis.core.fact_staking
    where tx_succeeded = 'TRUE' 
    and action = 'redelegate'
    and date_trunc('day', block_timestamp) between to_date((select date from votes_times)) -5 and  to_date((select date from votes_times)) + 7
    group by date, delegator_address, validator_address, REDELEGATE_SOURCE_VALIDATOR_ADDRESS
    ),
    validators_address as (
    select address, label, raw_metadata:"account_address" as account_address
    from osmosis.core.fact_validators 
    ),
    val_votes_aux as 
    (
    select voter, 
    proposal_id, 
    b.description, 
    rank() over (partition by voter, proposal_id order by block_timestamp desc) as rank
    from osmosis.core.fact_governance_votes a 
    left join osmosis.core.dim_vote_options b 
    on a.vote_option = b.vote_id
    where voter in (select distinct account_address from validators_address)
    and proposal_id =  '"""+str(proposal_choice) +"""'
    and tx_succeeded = 'TRUE'
    ),
    val_votes as (
    select voter, b.address, proposal_id, description from val_votes_aux a
    left join validators_address b 
    on a.voter = b.account_address
    where rank = 1 
    ),
    all_votes_per_proposal_and_validator as 
    (
    select 
    delegator_address, 
    case when b.voter is null then 'Did not vote'
    else b.description end as vote, 
    redelegate_source_validator_address as redelegated_from,
    validator_address as redelegated_to,
    case when redelegated_from = 'osmovaloper14n8pf9uxhuyxqnqryvjdr8g68na98wn5amq3e5' then 'Prism validator'
    else cc.label end as redelegated_from_label,
    cc.rank as redelegated_from_rank, 
    case when redelegated_to = 'osmovaloper14n8pf9uxhuyxqnqryvjdr8g68na98wn5amq3e5' then 'Prism validator'
    else c.label end as redelegated_to_label,
    c.rank as redelegated_to_rank,
    case when d.description is null then 'Did not vote'
    else d.description end as validator_redelegated_from_vote,
    case when e.description is null then 'Did not vote'
    else e.description end as validator_redelegated_to_vote,
    sum(amount) as total_amount
    from redelegations a 
    left join votes_proposal b 
    on a.delegator_address = b.voter
    left join osmosis.core.fact_validators c 
    on a.validator_address = c.address 
    left join osmosis.core.fact_validators cc 
    on a.redelegate_source_validator_address = cc.address 
    left join val_votes d 
    on a.validator_address = d.address 
    left join val_votes e 
    on a.redelegate_source_validator_address = d.address 
    group by 
    delegator_address, 
    vote,
    redelegated_from,
    redelegated_to,
    redelegated_from_label,
    redelegated_from_rank,
    redelegated_to_label,
    redelegated_to_rank, 
    validator_redelegated_from_vote,
    validator_redelegated_to_vote
    )
     select concat(validator_redelegated_from_vote, '->', validator_redelegated_to_vote) as casuistic,
  sum(total_amount)  as total_amount,
count(distinct delegator_address) as num_delegators from all_votes_per_proposal_and_validator
group by 1"""
 
    st.experimental_memo(ttl=1000000)
    @st.experimental_memo
    def compute_4(a):
        results=sdk.query(a)
        return results
      
    results_val_det = compute_4(sql_val_det)
    df_val_det = pd.DataFrame(results_val_det.records)
    
    st.text("")
    st.subheader("Redelegations overview")    
    st.write('')
    st.write('The following chart can be used as an introduction, in order to see how delegators redelegated overall for that proposal. When selecting a proposal ID, it shows how delegators redelegated from a validator vote do another validator vote. The same is shown on the side for number of redelegators.')
    st.write('')
    col1, col2 = st.columns(2) 
    
    fig1 = px.bar(df_val_det, x="casuistic", y="total_amount", color_discrete_sequence=px.colors.qualitative.Prism)
    fig1.update_layout(
    title="Selected proposal - redelegators validators choice",
    xaxis_title="Voting changes",
    yaxis_title="Total amount redelegated", 
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1 # gap between bars of the same location coordinate.
    )
    col1.plotly_chart(fig1, theme="streamlit", use_container_width=True)
   
    fig1 = px.bar(df_val_det, x="casuistic", y="num_delegators", color_discrete_sequence=px.colors.qualitative.Prism)
    fig1.update_layout(
    title="Selected proposal - number of delegators by choice",
    xaxis_title="Voting changes",
    yaxis_title="Number of redelegators", 
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1 # gap between bars of the same location coordinate.
    )
    col2.plotly_chart(fig1, theme="streamlit", use_container_width=True)   
        
# In[10]:


    st.text("")
    st.subheader("Redelegations to the selected validator")    
    st.write('')
    st.markdown("We can display how users behaved. For instance, we can first look at how many redelegations there were to the selected validator from other validators, and what option did those validators vote.")
    st.write('As stated at the start of the dashboard, the following numbers take into account the days between the voting started and 7 days after it ended.')
    sql4 = df_query_aux2 + str(proposal_choice) +"""'
    group by proposal_id
    ),
    -- This part is done to drop later all duplicate votes
    votes_proposal_aux as (
    select voter, proposal_id, vote_option, rank() over (partition by voter, proposal_id order by block_timestamp desc) as rank
    from osmosis.core.fact_governance_votes
    where tx_succeeded = 'TRUE'
    and proposal_id =  '"""+ str(proposal_choice)+"""'
    ),
    votes_proposal as 
    (select voter, 
    proposal_id,
    b.description
    from votes_proposal_aux a 
    left join osmosis.core.dim_vote_options b 
    on a.vote_option = b.vote_id
    where a.rank = 1
    and proposal_id =  '"""+str(proposal_choice) +"""'
    ),
    redelegations as 
    (
    select date_trunc('day', block_timestamp) as date,
    delegator_address,
    validator_address,
    redelegate_source_validator_address,
    sum(amount/pow(10, decimal)) as amount 
    from osmosis.core.fact_staking
    where tx_succeeded = 'TRUE' 
    and action = 'redelegate'
    and date_trunc('day', block_timestamp) between to_date((select date from votes_times)) - 5 and  to_date((select date from votes_times)) + 7
    group by date, delegator_address, validator_address, REDELEGATE_SOURCE_VALIDATOR_ADDRESS
    ),
    validators_address as (
    select address, label, raw_metadata:"account_address" as account_address
    from osmosis.core.fact_validators 
    ),
    val_votes_aux as 
    (
    select voter, 
    proposal_id, 
    b.description, 
    rank() over (partition by voter, proposal_id order by block_timestamp desc) as rank
    from osmosis.core.fact_governance_votes a 
    left join osmosis.core.dim_vote_options b 
    on a.vote_option = b.vote_id
    where voter in (select distinct account_address from validators_address)
    and proposal_id =  '"""+str(proposal_choice) +"""'
    and tx_succeeded = 'TRUE'
    ),
    val_votes as (
    select voter, b.address, proposal_id, description from val_votes_aux a
    left join validators_address b 
    on a.voter = b.account_address
    where rank = 1 
    ),
    all_votes_per_proposal_and_validator as 
    (
    select 
    delegator_address, 
    case when b.voter is null then 'Did not vote'
    else b.description end as vote, 
    redelegate_source_validator_address as redelegated_from,
    validator_address as redelegated_to,
    case when redelegated_from = 'osmovaloper14n8pf9uxhuyxqnqryvjdr8g68na98wn5amq3e5' then 'Prism validator'
    else cc.label end as redelegated_from_label,
    cc.rank as redelegated_from_rank, 
    case when redelegated_to = 'osmovaloper14n8pf9uxhuyxqnqryvjdr8g68na98wn5amq3e5' then 'Prism validator'
    else c.label end as redelegated_to_label,
    c.rank as redelegated_to_rank,
    case when d.description is null then 'Did not vote'
    else d.description end as validator_redelegated_from_vote,
    case when e.description is null then 'Did not vote'
    else e.description end as validator_redelegated_to_vote,
    sum(amount) as total_amount
    from redelegations a 
    left join votes_proposal b 
    on a.delegator_address = b.voter
    left join osmosis.core.fact_validators c 
    on a.validator_address = c.address 
    left join osmosis.core.fact_validators cc 
    on a.redelegate_source_validator_address = cc.address 
    left join val_votes d 
    on a.validator_address = d.address 
    left join val_votes e 
    on a.redelegate_source_validator_address = d.address 
    where c.label =  '"""+str(validator_choice) +"""'
    group by 
    delegator_address, 
    vote,
    redelegated_from,
    redelegated_to,
    redelegated_from_label,
    redelegated_from_rank,
    redelegated_to_label,
    redelegated_to_rank, 
    validator_redelegated_from_vote,
    validator_redelegated_to_vote
    )
    select * from all_votes_per_proposal_and_validator"""
  
  
    st.experimental_memo(ttl=1000000)
    @st.experimental_memo
    def compute_3(a):
        results=sdk.query(a)
        return results
      
    results4 = compute_3(sql4)
    df4 = pd.DataFrame(results4.records)




    if df4.empty:
        st.error("There were no redelegations from the selected validator and proposal ID")
    else: 

        df4_1 = df4.groupby(by=['redelegated_from_label','validator_redelegated_from_vote']).sum().reset_index(drop=False)
        df4_2 = df4.groupby(by=['redelegated_from_label','vote']).sum().reset_index(drop=False)

        fig1 = px.bar(df4_1, x="redelegated_from_label", y="total_amount", color="validator_redelegated_from_vote", color_discrete_sequence=px.colors.qualitative.Prism)
        fig1.update_layout(
        title="Selected proposal and validator - Vote choice and amount redelegated from other validators",
        xaxis_title="Validator redelegated from",
        yaxis_title="Amount (OSMO)",
        legend_title="Validator choice",
        xaxis_tickfont_size=14,
        yaxis_tickfont_size=14,
        bargap=0.15, # gap between bars of adjacent location coordinates.
        bargroupgap=0.1 # gap between bars of the same location coordinate.
        )
        st.plotly_chart(fig1, theme="streamlit", use_container_width=True)
        st.text("")
        st.markdown("Apart from this, we can also display how individual delegators who redelegated to the selected validatore voted. Thus, the following chart displays the same numbers as the chart above, but differenciating by delegators votes. This will allow any validator using this dashboard to see whether they believe their voting option influenced somehow their delegators or if the reason for redelegations seems to be a different one. ")
        st.text("") 

        fig1 = px.bar(df4_2, x="redelegated_from_label", y="total_amount", color="vote", color_discrete_sequence=px.colors.qualitative.Prism)
        fig1.update_layout(
        title="Selected proposal and validator - Vote choice and amount redelegated from other validators - Redelegator voting choice",
        xaxis_title="Validator redelegated from",
        yaxis_title="Amount (OSMO)",
        legend_title="Redelegator choice",
        xaxis_tickfont_size=14,
        yaxis_tickfont_size=14,
        bargap=0.15, # gap between bars of adjacent location coordinates.
        bargroupgap=0.1 # gap between bars of the same location coordinate.
        )
        st.plotly_chart(fig1, theme="streamlit", use_container_width=True)
        st.text("")
        st.markdown("Below I have also displayed the individual values which account for the previous charts, in case any further analysis is desired.")
        st.text("")
        st.text("")
        st.dataframe(df4) 

# In[12]:

 
    sql5 = df_query_aux2 + str(proposal_choice) +"""'
    group by proposal_id
    ),
    -- This part is done to drop later all duplicate votes
    votes_proposal_aux as (
    select voter, proposal_id, vote_option, rank() over (partition by voter, proposal_id order by block_timestamp desc) as rank
    from osmosis.core.fact_governance_votes
    where tx_succeeded = 'TRUE'
    and proposal_id =  '"""+ str(proposal_choice)+"""'
    ),
    votes_proposal as 
    (select voter, 
    proposal_id,
    b.description
    from votes_proposal_aux a 
    left join osmosis.core.dim_vote_options b 
    on a.vote_option = b.vote_id
    where a.rank = 1
    and proposal_id =  '"""+str(proposal_choice) +"""'
    ),
    redelegations as 
    (
    select date_trunc('day', block_timestamp) as date,
    delegator_address,
    validator_address,
    redelegate_source_validator_address,
    sum(amount/pow(10, decimal)) as amount 
    from osmosis.core.fact_staking
    where tx_succeeded = 'TRUE' 
    and action = 'redelegate'
    and date_trunc('day', block_timestamp) between to_date((select date from votes_times)) -5 and  to_date((select date from votes_times)) + 7
    group by date, delegator_address, validator_address, REDELEGATE_SOURCE_VALIDATOR_ADDRESS
    ),
    validators_address as (
    select address, label, raw_metadata:"account_address" as account_address
    from osmosis.core.fact_validators 
    ),
    val_votes_aux as 
    (
    select voter, 
    proposal_id, 
    b.description, 
    rank() over (partition by voter, proposal_id order by block_timestamp desc) as rank
    from osmosis.core.fact_governance_votes a 
    left join osmosis.core.dim_vote_options b 
    on a.vote_option = b.vote_id
    where voter in (select distinct account_address from validators_address)
    and proposal_id =  '"""+str(proposal_choice) +"""'
    and tx_succeeded = 'TRUE'
    ),
    val_votes as (
    select voter, b.address, proposal_id, description from val_votes_aux a
    left join validators_address b 
    on a.voter = b.account_address
    where rank = 1 
    ),
    all_votes_per_proposal_and_validator as 
    (
    select 
    delegator_address, 
    case when b.voter is null then 'Did not vote'
    else b.description end as vote, 
    redelegate_source_validator_address as redelegated_from,
    validator_address as redelegated_to,
    case when redelegated_from = 'osmovaloper14n8pf9uxhuyxqnqryvjdr8g68na98wn5amq3e5' then 'Prism validator'
    else cc.label end as redelegated_from_label,
    cc.rank as redelegated_from_rank, 
    case when redelegated_to = 'osmovaloper14n8pf9uxhuyxqnqryvjdr8g68na98wn5amq3e5' then 'Prism validator'
    else c.label end as redelegated_to_label,
    c.rank as redelegated_to_rank,
    case when d.description is null then 'Did not vote'
    else d.description end as validator_redelegated_from_vote,
    case when e.description is null then 'Did not vote'
    else e.description end as validator_redelegated_to_vote,
    sum(amount) as total_amount
    from redelegations a 
    left join votes_proposal b 
    on a.delegator_address = b.voter
    left join osmosis.core.fact_validators c 
    on a.validator_address = c.address 
    left join osmosis.core.fact_validators cc 
    on a.redelegate_source_validator_address = cc.address 
    left join val_votes d 
    on a.validator_address = d.address 
    left join val_votes e 
    on a.redelegate_source_validator_address = d.address 
    where cc.label =  '"""+str(validator_choice) +"""'
    group by 
    delegator_address, 
    vote,
    redelegated_from,
    redelegated_to,
    redelegated_from_label,
    redelegated_from_rank,
    redelegated_to_label,
    redelegated_to_rank, 
    validator_redelegated_from_vote,
    validator_redelegated_to_vote
    )
    select * from all_votes_per_proposal_and_validator"""

    st.experimental_memo(ttl=1000000)
    @st.experimental_memo
    def compute_4(a):
        results=sdk.query(a)
        return results
      
    results5 = compute_4(sql5)
    df5 = pd.DataFrame(results5.records)

    st.text("")
    st.subheader("Redelegations from the selected validator")
    st.text("")
    st.markdown("I`ve previously showed how users redelegating to the selected validator behaved. Now we can look it the other way around, this is, looking at the redelegations from the selected validator towards other validators, to see whether it is coherent with the previous result.")
    st.text("")


    if df5.empty:
        st.error("There were no redelegations from the selected validator and proposal ID")
    else: 


        df5_1 = df5.groupby(by=['redelegated_to_label','validator_redelegated_to_vote']).sum().reset_index(drop=False)
        df5_2 = df5.groupby(by=['redelegated_to_label','vote']).sum().reset_index(drop=False)

        fig1 = px.bar(df5_1, x="redelegated_to_label", y="total_amount", color="validator_redelegated_to_vote", color_discrete_sequence=px.colors.qualitative.Prism)
        fig1.update_layout(
        title="Selected proposal and validator - Vote choice and amount redelegated to other validators - Destination validator voting choice",
        xaxis_title="Validator redelegated to",
        yaxis_title="Amount (OSMO)",
        legend_title="Destination validator choice",
        xaxis_tickfont_size=14,
        yaxis_tickfont_size=14,
        bargap=0.15, # gap between bars of adjacent location coordinates.
        bargroupgap=0.1 # gap between bars of the same location coordinate.
        )
        st.plotly_chart(fig1, theme="streamlit", use_container_width=True)
        st.text("")
        st.markdown("Apart from this, we can also display how individual delegators who redelegated to the validators displayed above voted. Thus, the following chart displays the same numbers as the chart above, but differenciating by delegators votes. This will allow any validator using this dashboard to see whether they believe their voting option influenced somehow their delegators or if the reason for redelegations seems to be a different one. ")
        st.text("") 

        fig1 = px.bar(df5_2, x="redelegated_to_label", y="total_amount", color="vote", color_discrete_sequence=px.colors.qualitative.Prism)
        fig1.update_layout(
        title="Selected proposal and validator - Vote choice and amount redelegated to other validators - Redelegator voting choice",
        xaxis_title="Validator redelegated to",
        yaxis_title="Amount (OSMO)",
        legend_title="Redelegator choice",
        xaxis_tickfont_size=14,
        yaxis_tickfont_size=14,
        bargap=0.15, # gap between bars of adjacent location coordinates.
        bargroupgap=0.1 # gap between bars of the same location coordinate.
        )
        st.plotly_chart(fig1, theme="streamlit", use_container_width=True)
        st.text("")

    
    
# In[13]: 
    
    st.subheader("Delegators distribution for the selected validator") 
 
    st.experimental_memo(ttl=1000000)
    @st.experimental_memo
    def compute_6(a):
        results=sdk.query(a)
        return results
    
 
    sql7 = """ 
    with votes_times as 
   (select proposal_id, max(date_trunc('day', block_timestamp)) as date 
    from osmosis.core.fact_governance_votes
    where tx_succeeded = 'TRUE'
    group by proposal_id
    ),
    -- This part is done to drop later all duplicate votes
    votes_proposal_aux as (
    select voter, proposal_id, vote_option, rank() over (partition by voter, proposal_id order by block_timestamp desc) as rank
    from osmosis.core.fact_governance_votes
    where tx_succeeded = 'TRUE'
    and proposal_id in (select max(proposal_id) from votes_times)  
    ),
    votes_proposal as 
    (select voter, 
    proposal_id,
    b.description
    from votes_proposal_aux a 
    left join osmosis.core.dim_vote_options b 
    on a.vote_option = b.vote_id
    where a.rank = 1
    and proposal_id in (select max(proposal_id) from votes_times) 
    ),
    
    
    delegations as (
    select date_trunc('day', block_timestamp) as date,
    delegator_address,
    validator_address,
    sum(amount/pow(10, decimal)) as amount 
    from osmosis.core.fact_staking
    where tx_succeeded = 'TRUE' 
    and action = 'delegate'
    and date_trunc('day', block_timestamp) <= (select max(date) from votes_times)
    group by date, delegator_address, validator_address
    ),

    undelegations as (
    select date_trunc('day', block_timestamp) as date,
    delegator_address,
    validator_address,
    sum(amount/pow(10, decimal))*(-1) as amount 
    from osmosis.core.fact_staking
    where tx_succeeded = 'TRUE' 
    and action = 'undelegate'
    and date_trunc('day', block_timestamp) <= (select  max(date) from votes_times)
    group by date, delegator_address, validator_address
    ),

redelegations_to as 
(
select date_trunc('day', block_timestamp) as date,
delegator_address,
validator_address,
sum(amount/pow(10, decimal)) as amount 
from osmosis.core.fact_staking
where tx_succeeded = 'TRUE' 
and action = 'redelegate'
and date_trunc('day', block_timestamp) <= (select  max(date) from votes_times)
group by date, delegator_address, validator_address

),

redelegations_from as 
(
select date_trunc('day', block_timestamp) as date,
delegator_address,
redelegate_source_validator_address as validator_address,
sum(amount/pow(10, decimal))*(-1) as amount 
from osmosis.core.fact_staking
where tx_succeeded = 'TRUE' 
and action = 'redelegate'
and date_trunc('day', block_timestamp) <= (select  max(date) from votes_times)
group by date, delegator_address, redelegate_source_validator_address

),
    validators_address as (
    select address, label, raw_metadata:"account_address" as account_address
    from osmosis.core.fact_validators 
    ),
    val_votes_aux as 
    (
    select voter, 
    proposal_id, 
    b.description, 
    rank() over (partition by voter, proposal_id order by block_timestamp desc) as rank
    from osmosis.core.fact_governance_votes a 
    left join osmosis.core.dim_vote_options b 
    on a.vote_option = b.vote_id
    where voter in (select distinct account_address from validators_address)
    and proposal_id in (select max(proposal_id) from votes_times) 
    and tx_succeeded = 'TRUE'
    ),
    val_votes as (
    select voter, b.address, proposal_id, description from val_votes_aux a
    left join validators_address b 
    on a.voter = b.account_address
    where rank = 1 
    ),
    
    all_votes_per_proposal_and_validator as 
    (

    select 
    delegator_address, 
    case when b.voter is null then 'Did not vote'
    else b.description end as vote, 
    validator_address, 
    c.label, 
    c.rank,
    case when d.description is null then 'Did not vote'
    else d.description end as validator_vote,
    sum(amount) as total_amount
    from (
      select * from delegations
      union all 
      select * from undelegations 
      union all 
      select * from redelegations_to 
      union all 
      select * from redelegations_from
      ) a 
    left join votes_proposal b 
    on a.delegator_address = b.voter
    left join osmosis.core.fact_validators c 
    on a.validator_address = c.address 
    left join val_votes d 
    on a.validator_address = d.address  
    where c.label =  '"""+str(validator_choice) +"""'
        group by 
    delegator_address, 
    vote,
    validator_address,
    c.label,
    c.rank,
    validator_vote
    )

    select 
    case when total_amount between 0 and 10 then 'Between 0 and 10' 
    when total_amount between 10 and 50 then 'Between 10 and 50' 
    when total_amount between 50 and 100 then 'Between 50 and 100' 
    when total_amount between 100 and 500 then 'Between 100 and 500' 
    when total_amount between 500 and 2000 then 'Between 500 and 2000' 
    when total_amount between 2000 and 10000 then 'Between 2000 and 10000' 
    when total_amount between 10000 and 50000 then 'Between 10000 and 50000' 
    else 'More than 50000' end as grouped,
    case when total_amount between 0 and 10 then 0 
    when total_amount between 10 and 50 then 1 
    when total_amount between 50 and 100 then 2 
    when total_amount between 100 and 500 then 3 
    when total_amount between 500 and 2000 then 4
    when total_amount between 2000 and 10000 then 5 
    when total_amount between 10000 and 50000 then 6 
    else 7 end as grouped_numeric,
    count(distinct delegator_address) as num_users,
    sum(total_amount) as total_amount
    from all_votes_per_proposal_and_validator 
    where total_amount > 0
    group by 1, 2 """

    results7 = compute_6(sql7)
    df7 = pd.DataFrame(results7.records)
    df7 = df7.sort_values(by ='grouped_numeric', ascending = True)
    st.write('Finally, we can look at the current delegation distribution for the selected validator, both in terms of total Osmo delegated by type of user (small amount delegated users to big whales), and then by number of users which fall in each category.')
    col1, col2 = st.columns(2) 
    
    fig1 = px.bar(df7, x="grouped", y="total_amount", color_discrete_sequence=px.colors.qualitative.Prism)
    fig1.update_layout(
    title="Selected validator - current delegation distribution",
    xaxis_title="Category",
    yaxis_title="Total amount staked by group (OSMO)", 
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1 # gap between bars of the same location coordinate.
    )
    col1.plotly_chart(fig1, theme="streamlit", use_container_width=True)
    
    fig1 = px.bar(df7, x="grouped", y="num_users", color_discrete_sequence=px.colors.qualitative.Prism)
    fig1.update_layout(
    title="Selected validator - current delegation distribution",
    xaxis_title="Category",
    yaxis_title="Number of users", 
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1 # gap between bars of the same location coordinate.
    )
    col2.plotly_chart(fig1, theme="streamlit", use_container_width=True)
    
        
    sql7_new = """with votes_times as 
   (select proposal_id, max(date_trunc('day', block_timestamp)) as date 
    from osmosis.core.fact_governance_votes
    where tx_succeeded = 'TRUE'
    group by proposal_id
    ),
    -- This part is done to drop later all duplicate votes
    votes_proposal_aux as (
    select voter, proposal_id, vote_option, rank() over (partition by voter, proposal_id order by block_timestamp desc) as rank
    from osmosis.core.fact_governance_votes
    where tx_succeeded = 'TRUE'
    and proposal_id in (select max(proposal_id) from votes_times)  
    ),
    votes_proposal as 
    (select voter, 
    proposal_id,
    b.description
    from votes_proposal_aux a 
    left join osmosis.core.dim_vote_options b 
    on a.vote_option = b.vote_id
    where a.rank = 1
    and proposal_id in (select max(proposal_id) from votes_times) 
    ),
    
    
    delegations as (
    select date_trunc('day', block_timestamp) as date,
    delegator_address,
    validator_address,
    sum(amount/pow(10, decimal)) as amount 
    from osmosis.core.fact_staking
    where tx_succeeded = 'TRUE' 
    and action = 'delegate'
    and date_trunc('day', block_timestamp) <= (select max(date) from votes_times)
    group by date, delegator_address, validator_address
    ),
    undelegations as (
    select date_trunc('day', block_timestamp) as date,
    delegator_address,
    validator_address,
    sum(amount/pow(10, decimal))*(-1) as amount 
    from osmosis.core.fact_staking
    where tx_succeeded = 'TRUE' 
    and action = 'undelegate'
    and date_trunc('day', block_timestamp) <= (select  max(date) from votes_times)
    group by date, delegator_address, validator_address
    ),
redelegations_to as 
(
select date_trunc('day', block_timestamp) as date,
delegator_address,
validator_address,
sum(amount/pow(10, decimal)) as amount 
from osmosis.core.fact_staking
where tx_succeeded = 'TRUE' 
and action = 'redelegate'
and date_trunc('day', block_timestamp) <= (select  max(date) from votes_times)
group by date, delegator_address, validator_address
),
redelegations_from as 
(
select date_trunc('day', block_timestamp) as date,
delegator_address,
redelegate_source_validator_address as validator_address,
sum(amount/pow(10, decimal))*(-1) as amount 
from osmosis.core.fact_staking
where tx_succeeded = 'TRUE' 
and action = 'redelegate'
and date_trunc('day', block_timestamp) <= (select  max(date) from votes_times)
group by date, delegator_address, redelegate_source_validator_address
),
    validators_address as (
    select address, label, raw_metadata:"account_address" as account_address
    from osmosis.core.fact_validators 
    ),
    val_votes_aux as 
    (
    select voter, 
    proposal_id, 
    b.description, 
    rank() over (partition by voter, proposal_id order by block_timestamp desc) as rank
    from osmosis.core.fact_governance_votes a 
    left join osmosis.core.dim_vote_options b 
    on a.vote_option = b.vote_id
    where voter in (select distinct account_address from validators_address)
    and proposal_id in (select max(proposal_id) from votes_times) 
    and tx_succeeded = 'TRUE'
    ),
    val_votes as (
    select voter, b.address, proposal_id, description from val_votes_aux a
    left join validators_address b 
    on a.voter = b.account_address
    where rank = 1 
    ),

    allvotesinfo as (
    select 
    delegator_address, 
    case when b.voter is null then 'Did not vote'
    else b.description end as vote, 
    validator_address, 
    c.label, 
    c.rank,
    case when d.description is null then 'Did not vote'
    else d.description end as validator_vote,
    sum(amount) as total_amount
    from (
      select * from delegations
      union all 
      select * from undelegations 
      union all 
      select * from redelegations_to 
      union all 
      select * from redelegations_from
      ) a 
    left join votes_proposal b 
    on a.delegator_address = b.voter
    left join osmosis.core.fact_validators c 
    on a.validator_address = c.address 
    left join val_votes d 
    on a.validator_address = d.address  
    where c.label =  '"""+str(validator_choice) +"""'
        group by 
    delegator_address, 
    vote,
    validator_address,
    c.label,
    c.rank,
    validator_vote
    ),

last_table as (
 
    select tx_from, min(date_trunc('day', block_timestamp)) as mindate
from osmosis.core.fact_transactions
where tx_succeeded = 'TRUE' 
and tx_from in (select distinct delegator_address from allvotesinfo )
group by 1 
  )

select mindate, count(distinct tx_from) as num_delegators
from last_table
group by 1 """
    st.write('Another interesting thing is looking at the first transaction time of delegators for the selected validator. This might show some additional info to which users a validator is attracting.')
    results7_new = compute_6(sql7_new)
    df7_new = pd.DataFrame(results7_new.records)
    df7_new = df7_new.sort_values(by ='mindate', ascending = True)
    fig1 = px.scatter(df7_new, x='mindate', y='num_delegators',  size="num_delegators",
    color_discrete_sequence=px.colors.qualitative.Prism)
    fig1.update_layout(
    title="Selected validator - current delegators first transaction",
    xaxis_title="First transaction time ",
    yaxis_title="Number of users", 
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1 # gap between bars of the same location coordinate.
    )
    st.plotly_chart(fig1, theme="streamlit", use_container_width=True)
    
 
 
with tab3:
 
    SQL_QUERY_SANK_AUX = """   select distinct address, label, RANK() OVER (ORDER BY address DESC) AS RANK from osmosis.core.dim_labels
    where address in (select distinct validator_address from osmosis.core.fact_staking  ) 
    and address <> 'osmovaloper12l0vwef7w0xmkgktyqdzgd05jyq0lcuuqy2m8v'
    order by rank
    """  

    SQL_QUERY_SANK1 = """  with table_0 as ( select distinct address, label, RANK() OVER (ORDER BY address DESC) AS RANK from osmosis.core.dim_labels
    where address in (select distinct validator_address from osmosis.core.fact_staking ) 
    and address <> 'osmovaloper12l0vwef7w0xmkgktyqdzgd05jyq0lcuuqy2m8v')
    select b.label as from_validator, c.label as to_validator, d.RANK as from_validator_rank, e.rank as to_validator_rank, sum(amount/pow(10,decimal)) as amount_redelegated from osmosis.core.fact_staking a 
    left join  osmosis.core.dim_labels b 
    on a.redelegate_source_validator_address = b.address
    left join  osmosis.core.dim_labels c 
    on a.validator_address = c.address
    left join  table_0 d 
    on a.redelegate_source_validator_address = d.address
    left join  table_0 e 
    on a.validator_address = e.address
    where action = 'redelegate'
    and d.rank is not null
    and to_date(a.block_timestamp) >= '"""

    SQL_QUERY_SANK2 = """'

    group by from_validator, to_validator, from_validator_rank, to_validator_rank
    order by d.rank"""
 

    st.experimental_memo(ttl=1000000)
    @st.experimental_memo
    def compute_sank(a):
        results=sdk.query(a)
        return results

    results_sank_aux = compute_sank(SQL_QUERY_SANK_AUX)
    df_sank_aux = pd.DataFrame(results_sank_aux.records)   

    st.subheader("Sankey redelegation chart")
    st.write('Select a starting date and validator, returns a sankey chart with all redelegations from a selected validator to other validators, between the selected date and current date.')
    input_feature = st.date_input( "Introduce start date",  datetime.date(2023, 1, 1))   
    SQL_QUERY_SANK = SQL_QUERY_SANK1+ str(input_feature) + SQL_QUERY_SANK2

    results_sank = compute_sank(SQL_QUERY_SANK)
    df_sank = pd.DataFrame(results_sank.records)   




    randcolor = []
    for i in range(1,len(df_sank_aux['label']) + 1):

        randcolor.append("#{:06x}".format(random.randint(0, 0xFFFFFF))) 

    df_sank_aux['COLOR'] = randcolor


    keys_list =  df_sank_aux['rank']
    values_list = df_sank_aux['label']
    zip_iterator = zip(keys_list, values_list) 
    a_dictionary = dict(zip_iterator)



    df_sank_2 = pd.DataFrame(a_dictionary.items(), columns = ['rank','label'], index = keys_list)
    df_sank_2.index = df_sank_2.index
    df_sank_2 = df_sank_2.sort_index()






    with st.container():

        validator_choice_2 = st.selectbox("Choose a validator", options = df_sank['from_validator'].unique() )


        df_filtered = df_sank[df_sank['from_validator'] == validator_choice_2]
        df_filtered['Link color'] = 'rgba(127, 194, 65, 0.2)'
        df_filtered['from_validator_rank'] = df_filtered['from_validator_rank']-1
        df_filtered['to_validator_rank'] = df_filtered['to_validator_rank'] - 1

        link = dict(source = df_filtered['from_validator_rank'].values , target = df_filtered['to_validator_rank'].values, value = df_filtered['amount_redelegated'], color = df_sank_aux['COLOR'])
        node = dict(label = df_sank_2['label'].values, pad = 35, thickness = 10)




        data = go.Sankey(link = link, node = node)
        fig = go.Figure(data)
        fig.update_layout(
          hovermode = 'x', 
          font = dict(size = 20, color = 'white'), 
          paper_bgcolor= 'rgba(0,0,0,0)',
          width=1000, height=1300
         ) 

        st.plotly_chart(fig, use_container_width=True)  



st.header('Conclusions')
st.markdown('Osmosis governance and cosmos chains governance in general is always a hot and messy topic. I hope this dashboard draws attention to some insights otherwise hidden to plain sight, and that both delegators and validators can make use of it.')
st.markdown('Please reach out on twitter below if you feel something interesting is missing, or misleading.')
st.markdown('Streamlit App by [Jordi R.](https://twitter.com/RuspiTorpi/). Powered by Flipsidecrypto') 
 
