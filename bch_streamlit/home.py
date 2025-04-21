import streamlit as st

def bch_model_monitor_homepage():
    bch_model_monitor_markdown = """

        ## Welcome!
        *This is your no code solution to monitoring your models in Snowflake.*
        
        Here are just a few benefits:

        - Security
        - Ease of Access
        - Collaboration
        - Scale
        - Transparency
        - Results
        - Analytics

        ### Get Started Guide
    
        ##### 1. Model Dictionary Tab
        Find what models you have in your Snowflake environment.

        ##### 2. Model Accuracy Tab
        Learn how well your models are performing, accuracy and run performance
        Task Statistics
        Pipeline runtime

        #### 3. Summary Statistics Tab
        Model summary stats - column distributions

    """
    
    st.markdown(bch_model_monitor_markdown)

def main():
    ## App Start
    st.set_page_config(layout="wide")
    st.image('frostbanner.png', width = 300)
    bch_model_monitor_homepage()

if __name__ == "__main__":
    main()