FROM python:3.7

RUN apt-get update && apt-get install -y xvfb

# Install google chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN dpkg -i google-chrome-stable_current_amd64.deb; apt-get -fy install

# Set up Chromedriver Environment variables
ENV CHROMEDRIVER_VERSION 76.0.3809.68
ENV CHROMEDRIVER_DIR /chromedriver
RUN mkdir $CHROMEDRIVER_DIR

# Download and install Chromedriver
RUN wget -q --continue -P $CHROMEDRIVER_DIR "http://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip"
RUN unzip $CHROMEDRIVER_DIR/chromedriver* -d $CHROMEDRIVER_DIR

# set display port to avoid crash
ENV CHROME_BIN=/usr/bin/google-chrome
ENV DISPLAY=99

# Install selenium
RUN pip install selenium

WORKDIR /home