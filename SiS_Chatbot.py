#Don't Forget to import snowflake.core from anaconda packages
import streamlit as st
import pandas as pd
import json

from snowflake.snowpark.context import get_active_session
from snowflake.core import Root

session = get_active_session()  # Get the current credentials
pd.set_option("max_colwidth", None)
num_chunks = 20  # Number of chunks provided as context. Adjust as needed
slide_window = 7  # How many last conversations to remember. This is the slide window.

def create_prompt(myquestion, rag, chat_type, accessionid=None):

    
    root = Root(session)
    pmc_search_service = (
        root.databases["PMC_DATA"]
        .schemas["PMC_OA_OPENDATA"]
        .cortex_search_services["my_pmc_search_service"]
    )
    
    chat_history = get_chat_history(chat_type)

    if chat_history and st.session_state[f"{chat_type}_use_chat_history"]:
        question_summary = summarize_question_with_history(chat_history, myquestion, chat_type)
    else:
        question_summary = myquestion

    if rag == 1:
        if accessionid:
            similar_articles = pmc_search_service.search(
                query=question_summary,
                columns=["CHUNK", "ACCESSIONID"],
                filter={"@eq": {"accessionid": accessionid}},
                limit=num_chunks,
            )
        else:
            similar_articles = pmc_search_service.search(
                query=question_summary, columns=["CHUNK", "ACCESSIONID"], limit=num_chunks
            )

        similar_articles_resp = similar_articles.to_json()
        data = json.loads(similar_articles_resp)
        df_context = pd.json_normalize(data["results"])

        context_length = len(df_context) - 1
        prompt_context = ""
        for i in range(context_length):
            prompt_context += df_context._get_value(i, "CHUNK")

        prompt_context = prompt_context.replace("'", "")
        accessionid = df_context["ACCESSIONID"].unique()

        prompt = f"""
          'You are an expert assistant extracting information from context provided. 
           Answer the question based on the context.
           You offer a chat experience considering the information included in the CHAT HISTORY
           provided between <chat_history> and </chat_history> tags.
           Be concise and do not hallucinate. 
           If you don’t have the information just say so.
           
           Do not mention the CHAT HISTORY used in your answer.
           
           <chat_history>
           {chat_history}
           </chat_history>
          Context: {prompt_context}
          Question:  
           {myquestion} 
           Answer: '
           """
    else:
        prompt = f"""
        <chat_history>
        {chat_history}
        </chat_history>
         'Question:  
           {myquestion} 
           Answer: '
           """
        accessionid = "None"

    return prompt, accessionid

def complete(myquestion, model_name, rag=1, accessionid=None, chat_type="all"):
    prompt, accessionid = create_prompt(myquestion, rag,chat_type, accessionid)
    cmd = f"""
             select SNOWFLAKE.CORTEX.COMPLETE(?,?) as response
           """
    df_response = session.sql(cmd, params=[model_name, prompt]).collect()
    return df_response, accessionid

def display_response(contain,question, model, rag=0, accessionid=None, chat_type="all"):
            
    with contain.chat_message("assistant"):
        message_placeholder = st.empty()
        question = question.replace("'", "")
        with st.spinner(f"{model} thinking..."):
            response, accessionid = complete(question, model, rag, accessionid, chat_type)
            res_text = response[0].RESPONSE
            res_text = res_text.replace("'", "")

            if rag == 1 and accessionid is not None and accessionid.any() != "None":
                if len(accessionid) > 1:
                    res_text += "\n\nAssociated NCBI AccessionIDs that may be useful:\n"
                    for acc_id in accessionid:
                        article_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{acc_id}/"
                        res_text += f"[{acc_id}]({article_url})\n"
                else:
                    article_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{accessionid[0]}/"
                    res_text += f"\n\nAssociated NCBI AccessionID that may be useful:\n[{accessionid[0]}]({article_url})"

            message_placeholder.markdown(res_text)

        st.session_state[chat_type].append({"role": "assistant", "content": res_text})

def get_chat_history(chat_type="specific"):
    if chat_type in st.session_state:
        chat_history = st.session_state[chat_type]
    else:
        chat_history = []
    start_index = max(0, len(chat_history) - slide_window)
    return [msg["content"] for msg in chat_history[start_index:]]

def init_messages(chat_type="specific"):
    if chat_type not in st.session_state:
        st.session_state[chat_type] = []
        st.session_state[f"{chat_type}_use_chat_history"] = True
        st.session_state[f"{chat_type}_debug"] = False
    # Initialize chat history
    if 'specific_clear_conversation' not in st.session_state:
        st.session_state.specific_clear_conversation = False
    if 'all_clear_conversation' not in st.session_state:
        st.session_state.all_clear_conversation = False
        
    if st.session_state.specific_clear_conversation or "specific" not in st.session_state:
        st.session_state.specific = []
        st.session_state.specific_clear_conversation = False
    # Initialize chat history
    if st.session_state.all_clear_conversation or "all" not in st.session_state:
        st.session_state.all = []
        st.session_state.all_clear_conversation = False

def summarize_question_with_history(chat_history, question, chat_type):
    prompt = f"""
        Based on the chat history below and the question, generate a query that extend the question
        with the chat history provided. The query should be in natural language. 
        Answer with only the query. Do not add any explanation.
        
        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
        """

    cmd = """
            select snowflake.cortex.complete(?, ?) as response
          """
    df_response = session.sql(cmd, params=[st.session_state[f"{chat_type}_model_name"], prompt]).collect()
    summary = df_response[0].RESPONSE

    if st.session_state[f"{chat_type}_debug"]:
        st.text("Summary to be used to find similar chunks:")
        st.caption(summary)

    summary = summary.replace("'", "")
    return summary

def config_options(chat_type):  # Modified to take chat_type as argument
    st.selectbox('Select your model:', (
        'mixtral-8x7b', 'snowflake-arctic', 'mistral-large', 'llama3-8b', 
        'llama3-70b', 'reka-flash', 'mistral-7b', 'llama2-70b-chat', 'gemma-7b'), 
        key=f"{chat_type}_model_name")

    st.checkbox('Do you want that I remember the chat history?', key=f"{chat_type}_use_chat_history", value=True)
    st.checkbox('Debug: Click to see summary generated of previous conversation', key=f"{chat_type}_debug", value=False)
    st.button("Start Over", on_click=clear_conversation, key=f"{chat_type}_clear_button", args=(chat_type,))
    st.expander("Session State").write(st.session_state)

def clear_conversation(chat_type):
    st.session_state[chat_type] = []

########## Main code ###########
col1, col2= st.columns(2)
with col1:
    st.image("https://curehht.org/wp-content/uploads/2018/06/PubMed-Logo.jpg")
with col2:
        st.title("NCBI: Pubmed Central Open Access Chatbot")
st.subheader("Powered by ❄️ Cortex Search & Cortex LLMs")
st.info("Snowflake Cortex Search is currently in Private Preview")
st.write("Chat with NCBI Open Access Articles (~3.8M articles available)")

st.caption("Source: AWS Open Data Registry, The PMC Open Access (OA) Subset, includes all articles in PMC with a machine-readable Creative Commons license")
init_messages("all")
init_messages("specific")

tab1, tab2, tab3 = st.tabs(["Search Article Availability", "Chat with Specific Article", "Chat with All Articles"])

with tab1:
    browse_url = "https://www.ncbi.nlm.nih.gov/pmc/?term=open%20access%5Bfilter%5D"
    st.markdown(f"[Browse all available NCBI open access articles]({browse_url})")
    st.caption("Enter the NCBI PMC ID below to check if the article is available in the Cortex Search Service")
    accession_id_tab1 = st.text_input("Enter the Accession ID of the article ie: PMC9388525 ", key="accession_id_tab1")
    article_available=False
    if accession_id_tab1:
        cmd_check = f"select distinct accessionid from PMC_DATA.PMC_OA_OPENDATA.PMC_SERVICE_VW where accessionid='{accession_id_tab1}'"
        result_check = session.sql(cmd_check).collect()

        if result_check:
            st.write("This article has already been processed and is available to chat with.")
            article_available = True
        else:
            cmd_check_processed = (
                f"select distinct accessionid from PMC_DATA.PMC_OA_OPENDATA.OA_COMM_METADATA where accessionid not in (select distinct accessionid from PMC_DATA.PMC_OA_OPENDATA.PMC_SERVICE_VW) and accessionid='{accession_id_tab1}'"
            )
            result_check_processed = session.sql(cmd_check_processed).collect()

            if result_check_processed:
                st.write("The article is part of the NCBI open access, but needs to be added to the Cortex Search service")
                st.checkbox("Would you like to add the article to the service", key= "addtoservice", value=False)

                if st.session_state.addtoservice: 
                    with st.spinner("Processing the Article..."):
                        cmd_insert = f"""
                        insert into PMC_DATA.PMC_OA_OPENDATA.PMC_OA_CHUNKS_TABLE(relative_path, abs_path, etag, scoped_file_url, chunk)
                        with oa_comm as (select array_to_string(array_slice(split(key, '/'),1,4), '/') as relative_path , etag from PMC_DATA.PMC_OA_OPENDATA.OA_COMM_METADATA where accessionid='{accession_id_tab1}')
                        select relative_path, 
                            get_absolute_path(@PMC_DATA.PMC_OA_OPENDATA.PMC_OA_COMM_RAW, relative_path) as abs_path,
                            etag,
                            build_scoped_file_url(@PMC_DATA.PMC_OA_OPENDATA.PMC_OA_COMM_RAW, relative_path) as scoped_file_url,
                            func.chunk as chunk
                        from 
                            oa_comm,
                            TABLE(PMC_DATA.PMC_OA_OPENDATA.TXT_TEXT_CHUNKER(build_scoped_file_url(@PMC_DATA.PMC_OA_OPENDATA.PMC_OA_COMM_RAW, relative_path))) as func;
                        """
                        session.sql(cmd_insert).collect()
                    st.write("The article has just been submitted to search service it will be available as context in 2 mins")
                    article_available = True
            else:
                st.write("The article does not exist in the list of NCBI Open Access.")
                article_available = False

        if article_available:
            article_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{accession_id_tab1}/"
            st.markdown(f"[View the article on NCBI]({article_url})")

with tab2:
    accession_id_tab2 = st.text_input("Enter the Accession ID of the article ie: PMC9388525 ", key="accession_id_tab2")
    config_options("specific")
    chatcontainer= st.container(height=500)
    # Display chat messages from history on app rerun
    for message in st.session_state.specific:
        with chatcontainer.chat_message(message["role"]):
            st.markdown(message["content"])
    if accession_id_tab2:
        cmd_check = f"select distinct accessionid from PMC_DATA.PMC_OA_OPENDATA.PMC_SERVICE_VW where accessionid='{accession_id_tab2}'"
        result_check = session.sql(cmd_check).collect()
        if result_check:
            if chat_input := st.chat_input("Ask a question about the article:"):  # Moved outside the if
                st.session_state.specific.append({"role": "user", "content": chat_input})
                with chatcontainer.chat_message("user"):
                    st.markdown(chat_input)  
                display_response(chatcontainer,chat_input, st.session_state.specific_model_name, 1, accession_id_tab2, chat_type="specific")
        else:
            st.info("Article not found in search service, Please confirm availability in first tab.")


with tab3:
    docs_count = session.sql(
        "select count(distinct ACCESSIONID) as count from PMC_DATA.PMC_OA_OPENDATA.PMC_SERVICE_VW"
    ).collect()[0]["COUNT"]
    st.write(f"This is the number of articles you have available: {docs_count}")

    config_options("all")  # Pass chat_type as argument

    chatcontainer2= st.container(height=500)
    # Display chat messages from history on app rerun
    for message in st.session_state.all:
        with chatcontainer2.chat_message(message["role"]):
            st.markdown(message["content"])
     # Chat UI for Tab 3 (All Articles)
    if chat_input := st.chat_input("Ask questions against all the articles in search service"):  # Moved outside the if
        st.session_state.all.append({"role": "user", "content": chat_input})
        with chatcontainer2.chat_message("user"):
            st.markdown(chat_input)  # Display user message
        display_response(chatcontainer2,chat_input, st.session_state.all_model_name, 1, chat_type="all")
st.caption("Owner: Namitha Patil, Please reach out to namitha.patil@snowflake.com for any questions. ")