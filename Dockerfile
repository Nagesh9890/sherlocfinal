# Base image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Clone the GitHub repository
RUN git clone https://github.com/vishalsiram/sherlock_pipeline.git /app/repository

# Debug - List files in the repository directory
RUN ls -la /app/repository

# Copy the code file and pickle files from the cloned repository
COPY app.py /app/
COPY tfidf_remitter_name_m3.pkl /app/
COPY tfidf_source_m3.pkl /app/
COPY tfidf_base_txn_text_m3.pkl /app/
COPY tfidf_mode_m3.pkl /app/
COPY tfidf_benef_name_m3.pkl /app/
COPY classifier_m3.pkl /app/

# Debug - List files in the app directory
RUN ls -la /app

# Copy the requirements.txt file from the cloned repository
COPY requirements.txt /app/

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port on which the Flask app will run (if needed)
EXPOSE 5000

# Run the Flask app
CMD ["python", "app.py"]

