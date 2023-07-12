# Use this for ARM64 / macOS M1:
#FROM python:3.10-slim

# Use this for AMD64:
FROM --platform=linux/amd64 python:3.10-slim

# Install Nginx
RUN apt-get update && \
    apt-get install -y nginx nano && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Create directories for data
RUN mkdir Consumos
RUN mkdir OMIE_Data
RUN mkdir ERedes_profiles
RUN mkdir Logs

# Create directories for Code Apps
RUN mkdir tracker-server

# Copy the backend of the application code into the container
COPY ./omie-server /app/tracker-server

# Copy the OMIE data into the container
COPY ./OMIE_Data /app/OMIE_Data
COPY ./ERedes_profiles /app/ERedes_profiles

# Copy the web app client files to the Nginx root directory
COPY ./omie-client/app.min.js /var/www/html
COPY ./omie-client/index.html /var/www/html
COPY ./omie-client/styles.css /var/www/html
COPY ./omie-client/sitemap.xml /var/www/html
COPY ./omie-client/bootstrap-slider.min.css /var/www/html
COPY ./omie-client/bootstrap-slider.min.js /var/www/html

# Remove the default Nginx configuration file and add a custom one
RUN rm /etc/nginx/sites-enabled/default
COPY nginx.conf /etc/nginx/sites-enabled/

# Make port 80 available to the world outside this container
EXPOSE 80

# Make port 5000 available to the world outside this container
EXPOSE 5000

# Copy the Supervisor configuration file into the container
COPY supervisord.conf /app/.
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Run Supervisor when the container launches
CMD ["/usr/local/bin/supervisord"]
