# Use the official Python base image
FROM python:3.10-slim

# Set the working directory
WORKDIR  /app

#Copy source files to the container
COPY . /app

# Install pip dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port 5000
EXPOSE 5000

CMD ["python", "conductor.py"]