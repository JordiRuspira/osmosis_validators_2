#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
st.write('- You then are presented with three tabs. The first one is just an overview of how individual validators voted on that proposal. The second one allows you to see in big numbers how delegators voted and the quorum it would have reached if only their vote counted (over total percentage of Osmo staked). The last tab in this section allows for users to see this data for all proposals already voted. Spoiler: almost no proposal would have reached quorum if it werent for the delegated proof of stake system.')
st.write('- After this section come the insights for specific validators. Once a proposal ID has been selected in the previous section, you should choose a validator from the select box to see how delegators reacted for that validator and proposal. The numbers displayed here show movement between the go live of the proposal and 7 days after the ending of the vote. Again, there are multiple tabs to show different information.')
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
@st.cache_data
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
st.subheader('Selecting a proposal ID')

proposal_choice = '427'
proposal_choice = st.selectbox("Select a proposal", options = df1['proposal_id'].unique() ) 

st.write('We can see how validators voted on the selected proposal, ordered by rank and voting option.') 
st.write('')

df0_fil = df0[df0['proposal_id'] == proposal_choice]

tab1, tab2 = st.tabs(["Validator vote for the selceted proposal", "Delegator vote and quorum if meeted"])

# In[7]:

with tab1:
    

    fig1 = px.bar(df0_fil, x="label", y="value", color="description", color_discrete_sequence=px.colors.qualitative.Vivid)
    fig1.update_layout(
    title='Validator voting choice for selected proposal',
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1 # gap between bars of the same location coordinate.
    )
    st.plotly_chart(fig1, theme="streamlit", use_container_width=True)

with tab2:
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
   
    @st.cache_data
    def compute_1(a):
        results=sdk.query(a)
        return results
      
    results6 = compute_1(sql6)
    df6 = pd.DataFrame(results6.records)
    df6.info()
    
    col1, col2 = st.columns(2) 
    
    fig1 = px.bar(df6, x="casuistic", y="percentage", color_discrete_sequence=px.colors.qualitative.Vivid)
    fig1.update_layout(
    title="Selected proposal and delegator total percentage",
    xaxis_title="Vote/did not vote",
    yaxis_title="Percentage of total staked at the time", 
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1 # gap between bars of the same location coordinate.
    )
    col1.plotly_chart(fig1, theme="streamlit", use_container_width=True)
   
    fig1 = px.bar(df6, x="casuistic", y="total_amount_group", color_discrete_sequence=px.colors.qualitative.Vivid)
    fig1.update_layout(
    title="Selected proposal and delegator votes",
    xaxis_title="Vote/did not vote",
    yaxis_title="Amount (OSMO) staked", 
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1 # gap between bars of the same location coordinate.
    )
    col2.plotly_chart(fig1, theme="streamlit", use_container_width=True)   

# In[8]:


st.markdown(""" <style> div.css-12w0qpk.e1tzin5v2{
 background-color: #f5f5f5;
 border: 2px solid;
 padding: 10px 5px 5px 5px;
 border-radius: 10px;
 color: #ffc300;
 box-shadow: 10px;
}
div.css-1r6slb0.e1tzin5v2{
 background-color: #f5f5f5;
 border: 2px solid; /* #900c3f */
 border-radius: 10px;
 padding: 10px 5px 5px 5px;
 color: green;
}
div.css-50ug3q.e16fv1kl3{
 font-weight: 900;
} 
</style> """, unsafe_allow_html=True)

st.markdown(""" <style> div.css-ocqkz7.e1tzin5v4{
 background-color: #f5f5f5;
 border: 2px solid;
 padding: 10px 5px 5px 5px;
 border-radius: 10px;
 color: #ffc300;
 box-shadow: 10px;
}
div.css-keje6w.ce1tzin5v2{
 background-color: #f5f5f5;
 border: 2px solid; /* #900c3f */
 border-radius: 10px;
 padding: 10px 5px 5px 5px;
 color: orange;
}
div.css-12ukr4l.e1tzin5v0{
 font-weight: 900;
} 
</style> """, unsafe_allow_html=True)

import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.subheader('Selecting a validator')
st.write('Use the selectbox below to select the validator you want to analyze.') 
st.write('')
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
 
# In[10]:
tab1, tab2, tab3 = st.tabs(["Redelegations from the selected validator", "Redelegations to the selected validator","Table data"])

# In[11]:
with tab1:
 
    st.markdown("We can display how users behaved. For instance, we can first look at how many redelegations there were from the selected validator towards other validators, and what option did those validators vote.")
 
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
    @st.cache_data
    def compute_3(a):
        results=sdk.query(a)
        return results
      
    results4 = compute_3(sql4)
    df4 = pd.DataFrame(results4.records)
    df4.info()




    if df4.empty:
        st.markdown("There were no redelegations from the selected validator and proposal ID")
    else: 

        df4_1 = df4.groupby(by=['redelegated_from_label','validator_redelegated_from_vote']).sum().reset_index(drop=False)
        df4_2 = df4.groupby(by=['redelegated_from_label','vote']).sum().reset_index(drop=False)

        fig1 = px.bar(df4_1, x="redelegated_from_label", y="total_amount", color="validator_redelegated_from_vote", color_discrete_sequence=px.colors.qualitative.Vivid)
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
        st.markdown("Apart from this, we can also display how individual delegators who redelegated to the validators displayed above voted. Thus, the following chart displays the same numbers as the chart above, but differenciating by delegators votes. This will allow any validator using this dashboard to see whether they believe their voting option influenced somehow their delegators or if the reason for redelegations seems to be a different one. ")
        st.text("") 

        fig1 = px.bar(df4_2, x="redelegated_from_label", y="total_amount", color="vote", color_discrete_sequence=px.colors.qualitative.Vivid)
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
with tab2:
 
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
    @st.cache_data
    def compute_4(a):
        results=sdk.query(a)
        return results
      
    results5 = compute_4(sql5)
    df5 = pd.DataFrame(results5.records)
    df5.info()

    st.text("")
    st.subheader("Redelegations to the selected validator")
    st.text("")
    st.markdown("We previously showed how users redelegating from the selected validator behaved. Now we can do the same exercise but the other way around. We can see how votes coming from other validators to the selected one behave, and see if it is coherent with the previous result.")
    st.text("")


    if df5.empty:
        st.markdown("There were no redelegations to the selected validator and proposal ID")
    else: 


        df5_1 = df5.groupby(by=['redelegated_to_label','validator_redelegated_to_vote']).sum().reset_index(drop=False)
        df5_2 = df5.groupby(by=['redelegated_to_label','vote']).sum().reset_index(drop=False)

        fig1 = px.bar(df5_1, x="redelegated_to_label", y="total_amount", color="validator_redelegated_to_vote", color_discrete_sequence=px.colors.qualitative.Vivid)
        fig1.update_layout(
        title="Selected proposal and validator - Vote choice and amount redelegated to other validators - Destination validator voting choice",
        xaxis_title="Validator redelegated from",
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

        fig1 = px.bar(df5_2, x="redelegated_to_label", y="total_amount", color="vote", color_discrete_sequence=px.colors.qualitative.Vivid)
        fig1.update_layout(
        title="Selected proposal and validator - Vote choice and amount redelegated to other validators - Redelegator voting choice",
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
        
# In[13]: 
with tab3: 
 
    st.experimental_memo(ttl=1000000)
    @st.cache_data
    def compute_5(a):
        results=sdk.query(a)
        return results

    results3 = compute_5(sql3)
    df3 = pd.DataFrame(results3.records)
    df3.info()
    st.dataframe(df3) 
    st.text("")
    st.markdown("The way to read the table above is as follows. For the selected proposal ID and validator, we see first the validator vote, followed by the number of its delegators who voted for any proposal, if they voted, and the amount they account for of that specific delegator.")
    st.text("")

 
   



st.subheader('Conclusions')
st.markdown('**Osmosis Governance** blabla.')
st.markdown('The most interesting things we have found are:')
st.write('- Blah')
 
 
