# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set the environment variable for the Pickle file path (you can change the default if needed)
ARG PICKLE_FILE_PATH=/app/your_model.pkl
ENV PICKLE_FILE_PATH ${PICKLE_FILE_PATH}

# Set the environment variable for the port (you can change the default if needed)
ARG PORT=8888
ENV PORT ${PORT}

# Expose the specified port
EXPOSE ${PORT}

# Define environment variable
ENV NAME World

# Run app.py when the container launches
CMD ["python", "app.py"]
