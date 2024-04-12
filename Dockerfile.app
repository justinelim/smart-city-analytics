FROM python:3.12.2

# Set the working directory in the container
WORKDIR /usr/src
# Copy the current directory contents into the container at /usr/src/app
COPY . .

# RUN apt-get install wget

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variable
ENV NAME World

ENTRYPOINT [ "python", "app.py" ]