# Base image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Clone the GitHub repository
RUN git clone https://github.com/vishalsiram/sherlock_pipeline.git /app/repository

# Copy the code file and pickle files from the cloned repository
COPY /app/repository/<code_file_name.py> /app/
COPY /app/repository/tfidf_remitter_name_m3.pkl /app/
COPY /app/repository/tfidf_source_m3.pkl /app/
COPY /app/repository/tfidf_base_txn_text_m3.pkl /app/
COPY /app/repository/tfidf_mode_m3.pkl /app/
COPY /app/repository/tfidf_benef_name_m3.pkl /app/
COPY /app/repository/classifier_m3.pkl /app/

# Copy the requirements.txt file from the cloned repository
COPY /app/repository/requirements.txt /app/

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port on which the Flask app will run (if needed)
EXPOSE 5000

# Run the Flask app
CMD ["python", "app.py"]
