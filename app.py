# -*- coding: utf-8 -*-
"""
Created on Tue Apr  4 02:09:52 2023

@author: Vishal
"""

# Import required libraries
from sklearn.multioutput import MultiOutputClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.multioutput import MultiOutputClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
import pickle
from flask import Flask, request, jsonify
import json
import re

app = Flask(__name__)

# Define a helper function to clean text
def clean_text(text):
    # Remove special characters and symbols
    text = re.sub('[^a-zA-Z\s]+', '', text)
    # Convert text to lowercase
    text = text.lower()
    # Remove numbers
    text = re.sub('\d+', '', text)
    return text

def custom_tokenizer(text):
    # split the text and value using regular expression
    import re
    pattern = re.compile(r'[a-zA-Z]+\d+')
    text_and_value = pattern.findall(text)
    return text_and_value

# Load the TF-IDF vectorizers from the pickle files
with open('tfidf_remitter_name_m3.pkl', 'rb') as file:
    tfidf_remitter_name = pickle.load(file)
    
with open('tfidf_source_m3.pkl', 'rb') as file:
    tfidf_source = pickle.load(file)

with open('tfidf_base_txn_text_m3.pkl', 'rb') as file:
    tfidf_base_txn_text = pickle.load(file)

with open('tfidf_mode_m3.pkl', 'rb') as file:
    tfidf_mode = pickle.load(file)

with open('tfidf_benef_name_m3.pkl', 'rb') as file:
    tfidf_benef_name = pickle.load(file)

with open('classifier_m3.pkl', 'rb') as file:
    classifier3 = pickle.load(file)



@app.route('/bulk_req', methods=['POST'])
def pred():
    data = request.get_json(force=True)
    source_tfidf = tfidf_source.transform([clean_text(item.get('source', '')) for item in data])
    remitter_name_tfidf = tfidf_remitter_name.transform([clean_text(item.get('remitter_name', '')) for item in data])
    base_txn_text_tfidf = tfidf_base_txn_text.transform([clean_text(item.get('base_txn_text', '')) for item in data])
    mode_tfidf = tfidf_mode.transform([clean_text(item.get('mode', '')) for item in data])
    benef_name_tfidf = tfidf_benef_name.transform([clean_text(item.get('benef_name', '')) for item in data])
    
    input_tfidf = pd.concat([pd.DataFrame(source_tfidf.toarray()),
                             pd.DataFrame(remitter_name_tfidf.toarray()),
                             pd.DataFrame(base_txn_text_tfidf.toarray()), 
                             pd.DataFrame(mode_tfidf.toarray()), 
                             pd.DataFrame(benef_name_tfidf.toarray())], axis=1)
    
    predictions = classifier3.predict(input_tfidf)
    
    output = []
    for i, categories in enumerate(predictions):
        category_level1 = categories[0]
        category_level2 = categories[1]
        item = data[i].copy()
        item['category_level1'] = category_level1
        item['category_level2'] = category_level2
        output.append(item)
    
    return jsonify(output)


if __name__ == '__main__':
    app.run(debug=True,host="0.0.0.0", port=5000)