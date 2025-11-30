# File Location: C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\the_OracleChambers\src\fed_speak\inputs\fedspeak_loader.py

from pathlib import Path
import pandas as pd
import requests
from bs4 import BeautifulSoup
import spacy
from transformers import BertTokenizer, BertForSequenceClassification
import torch
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Define the data storage path
DATA_DIR = Path(__file__).resolve().parents[3] / "data" / "processed"
nlp = spacy.load("en_core_web_sm")  # Load the spaCy model for sentence segmentation
sia = SentimentIntensityAnalyzer()  # Initialize the SentimentIntensityAnalyzer (VADER)

# Load the pre-trained BERT model and tokenizer from HuggingFace
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
model = BertForSequenceClassification.from_pretrained('bert-base-uncased', num_labels=3)  # 3 labels: hawkish, dovish, neutral

def encode_sentences(sentences):
    """Encode the sentences for BERT model input."""
    inputs = tokenizer(sentences, padding=True, truncation=True, return_tensors='pt')
    return inputs

def classify_with_bert(sentences):
    """
    Use a fine-tuned BERT model to classify sentences as hawkish, dovish, or neutral.
    """
    inputs = encode_sentences(sentences)
    outputs = model(**inputs)
    predictions = torch.argmax(outputs.logits, dim=-1)
    return predictions

# Define the function to load Fed Speak leaves from Parquet
def load_fed_leaves() -> pd.DataFrame:
    """
    Load the Fed Speak leaves from the processed data (e.g., fed_leaves.parquet).
    This file contains aggregated sentiment scores and tone classifications.
    """
    fed_leaves_path = DATA_DIR / "fed_leaves.parquet"
    if fed_leaves_path.exists():
        fed_leaves = pd.read_parquet(fed_leaves_path)
        return fed_leaves
    else:
        raise FileNotFoundError(f"The file {fed_leaves_path} does not exist. Please run the previous steps.")

def scrape_fomc_events() -> pd.DataFrame:
    """
    Scrape the Federal Reserve website for event metadata (FOMC documents).
    Returns a DataFrame containing event URLs, types, and dates.
    """
    base_url = "https://www.federalreserve.gov/monetarypolicy.htm"
    
    response = requests.get(base_url)  # Fetch the page
    soup = BeautifulSoup(response.text, 'html.parser')  # Parse the content
    
    events = []
    for link in soup.find_all('a', href=True):
        if "statement" in link['href'] or "minutes" in link['href']:
            event_url = "https://www.federalreserve.gov" + link['href']
            event_date = link.find_next('span').text  # Extract date
            events.append({
                'event_url': event_url,
                'event_date': event_date,
                'event_type': 'statement' if 'statement' in link['href'] else 'minutes'
            })
    
    events_df = pd.DataFrame(events)
    events_df['event_date'] = pd.to_datetime(events_df['event_date'])
    
    # Save to Parquet for later processing
    events_df.to_parquet(DATA_DIR / "fed_events.parquet", index=False)
    return events_df

def split_into_sentences(docs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Split FOMC document text into sentences and assign metadata.
    Returns a DataFrame containing event_id, sentence, and sentence position.
    """
    sentences = []
    
    for _, row in docs_df.iterrows():
        doc = nlp(row['doc_text'])  # Apply spaCy NLP pipeline
        for sent in doc.sents:  # Split into sentences
            sentences.append({
                'event_id': row['event_id'],
                'sentence': sent.text,
                'position_in_doc': sent.start
            })
    
    sentences_df = pd.DataFrame(sentences)
    sentences_df.to_parquet(DATA_DIR / "fed_canonical_sentences.parquet", index=False)
    return sentences_df

def process_fomc_minutes(fomc_minutes_df):
    """
    Process the FOMC Minutes, tokenize, and classify sentences using BERT.
    """
    sentences = fomc_minutes_df['sentence'].tolist()
    
    # Classify the sentences using BERT
    predictions = classify_with_bert(sentences)
    
    # Convert predictions to readable format
    sentiment_labels = ['hawkish', 'dovish', 'neutral']
    classified_sentences = [sentiment_labels[prediction] for prediction in predictions]
    
    # Add the predictions to the DataFrame
    fomc_minutes_df['bert_tone'] = classified_sentences
    fomc_minutes_df.to_parquet(DATA_DIR / "fomc_minutes_bert_classified.parquet", index=False)
    return fomc_minutes_df

def process_fed_sep(fed_sep_df):
    """
    Process the Fed SEP (Economic Projections) using BERT to classify latent sentiment.
    """
    sentences = fed_sep_df['sentence'].tolist()
    
    # Classify the sentences using BERT
    predictions = classify_with_bert(sentences)
    
    # Convert predictions to readable format
    sentiment_labels = ['hawkish', 'dovish', 'neutral']
    classified_sentences = [sentiment_labels[prediction] for prediction in predictions]
    
    # Add the predictions to the DataFrame
    fed_sep_df['bert_tone'] = classified_sentences
    fed_sep_df.to_parquet(DATA_DIR / "fed_sep_bert_classified.parquet", index=False)
    return fed_sep_df

def generate_fed_leaves(sentences_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate sentence-level sentiment into document-level 'leaves' for policy regime classification.
    This method calculates hawkish/dovish tone and stores the results.
    """
    aggregated_scores = {
        'hawkish': 0,
        'dovish': 0,
        'neutral': 0
    }
    
    for _, row in sentences_df.iterrows():
        score = row['compound']
        tone = row['bert_tone']
        
        if tone == 'hawkish':
            aggregated_scores['hawkish'] += score
        elif tone == 'dovish':
            aggregated_scores['dovish'] += score
        else:
            aggregated_scores['neutral'] += score
    
    leaves_summary = {
        'hawkish_score': aggregated_scores['hawkish'],
        'dovish_score': aggregated_scores['dovish'],
        'neutral_score': aggregated_scores['neutral']
    }
    
    leaves_df = pd.DataFrame([leaves_summary])
    leaves_df.to_parquet(DATA_DIR / "fed_leaves.parquet", index=False)
    return leaves_df

def integrate_fed_speak_into_spine() -> dict:
    """
    Integrate the Fed Speak leaves into the macro-state model (the_Spine).
    This function returns the updated macroeconomic probabilities (e.g., inflation vs. growth).
    """
    fed_leaves = load_fed_leaves()  # Load Fed Speak leaves from processed data
    latest_leaf = fed_leaves.iloc[-1]
    
    inflation_risk = latest_leaf['hawkish_score'] / (latest_leaf['hawkish_score'] + latest_leaf['dovish_score'])
    growth_risk = latest_leaf['dovish_score'] / (latest_leaf['hawkish_score'] + latest_leaf['dovish_score'])
    
    return {
        'inflation_risk': inflation_risk,
        'growth_risk': growth_risk
    }




