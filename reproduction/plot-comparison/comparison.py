import streamlit as st
from typing import List
import os
import base64
import operator

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

PAPER_DIRECTORY = os.path.join(SCRIPT_DIR, "../original-plots")
REPRODUCTION_DIRECTORY = os.path.join(SCRIPT_DIR, "../data/output")

height = st.number_input(label="PDF Height [px]", min_value=0, value=200)

def get_files(directory: str) -> List[str]:
    return [f for f in os.listdir(directory) if f.endswith(".pdf")]

paper_plots = get_files(PAPER_DIRECTORY)
repro_plots = get_files(REPRODUCTION_DIRECTORY)

common_plots = list(set(paper_plots) & set(repro_plots))

def display_pdf(file):
    with open(os.path.realpath(file), "rb") as f:
        base64_pdf = base64.b64encode(f.read()).decode('utf-8')
   
    pdf_display = F'<iframe src="data:application/pdf;base64,{base64_pdf}" width="100%" height="{height}px" type="application/pdf"></iframe>'
    st.markdown(pdf_display, unsafe_allow_html=True)

if len(common_plots) == 0:
    st.write("No plots to compare.")
else:
    st.write("Plots available in paper but not in reproduction:")
    st.write(set(paper_plots) - set(repro_plots))
    st.write("---")
    st.write("Plots available in repro but not in paper:")
    st.write(set(repro_plots) - set(paper_plots))
    st.write("---")

    left, right = st.columns([1, 1])
    left.write("# Paper plots")
    right.write("# Reproduction plots")
    for f in sorted(common_plots):
        st.write(f)
        left, right = st.columns([1, 1])
        with left:
            display_pdf(os.path.join(PAPER_DIRECTORY, f))
        with right:
            display_pdf(os.path.join(REPRODUCTION_DIRECTORY, f))

