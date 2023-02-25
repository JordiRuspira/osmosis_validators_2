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
st.title('Osmosis Governance')



# In[20]:


st.markdown('The goal of this dashboard for validators to have a better understanding and insights of how their voting options impacts on their delegators.')

st.markdown('How to use this tool: you can select a proposal ID on Osmosis and a Validator, and it will give you insights on how delegators behaved after the proposal.')

st.markdown('When you select a proposal ID, the numbers displayed show movement between the go live of the proposal and 7 days after the ending of the vote.')

 

# In[10]:

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

# In[11]:


st.experimental_memo(ttl=1000000)
@st.cache
def compute(a):
    results=sdk.query(a)
    return results

results0 = compute(sql0)
df0 = pd.DataFrame(results0.records)
df0.info()

results1 = compute(sql1)
df1 = pd.DataFrame(results1.records)
df1.info()

results1 = compute(sql2)
df2 = pd.DataFrame(results2.records)
df2.info()


# In[22]:

import math


st.subheader('Selecting a proposal ID')
st.write('We can see how validators voted on the selected proposal, ordered by rank and voting option.') 
st.write('')
 
proposal_choice = '427'
proposal_choice = st.selectbox("Select a proposal", options = df1['proposal_id'].unique() ) 
df0_fil = df0[df0['proposal_id'] == str(proposal_choice)]


fig1 = px.bar(df0_fil, x="label", y="value", color="description", color_discrete_sequence=px.colors.qualitative.Vivid)
fig1.update_layout(
    title='Validator voting choice for selected proposal',
    xaxis_tickfont_size=14,
    yaxis_tickfont_size=14,
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1 # gap between bars of the same location coordinate.
)
st.plotly_chart(fig1, theme="streamlit", use_container_width=True)



# In[43]:


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





st.subheader('Conclusions')
st.markdown('**Near Protocol** has been doing good things over the past year, growing fast in web3 sector. It is clear that it is one of hte most interesting protocol deditacted to this sector to take into account for the upcoming years.')
st.markdown('The most interesting things we have found are:')
st.write('- 2k developers joined NEAR Protocol so far, being the major of them active in 2022')
st.write('- More than 50% of github Repos and almost 50% of PRs were created during 2022')
st.write('- Developers activity has been stable over time showing less activity during weekend')
st.write('- There have been more active contributors than members, or collaborators.')
st.write('- All of them increased in both number and activity over 2022')
st.write('- There are more one-time and part-time job users than full-time. But all of them have similar contributions.')
st.write('- There has been ~10% developer retention over the past month (29/300).')
st.write('- The major of developers stay active less than a month but there are almost 200 being active for more than 6 months.')
st.write('')
st.markdown('This app has been done by **_Adrià Parcerisas_**, a PhD Biomedical Engineer related to Machine Learning and Artificial intelligence technical projects for data analysis and research, as well as dive deep on-chain data analysis about cryptocurrency projects. You can find me on [Twitter](https://twitter.com/adriaparcerisas)')
st.write('')
st.markdown('The full sources used to develop this app can be found to the following link: [Github link](https://github.com/adriaparcerisas/Near-developer-activity)')
st.markdown('_Powered by [Flipside Crypto](https://flipsidecrypto.xyz) and [MetricsDAO](https://metricsdao.notion.site)_')
