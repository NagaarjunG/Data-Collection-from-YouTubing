# Data-Collection-from-YouTubing
An interactive Python application for collecting and analyzing YouTube channel data using the YouTube Data API.



YouTube Data Harvesting and Warehousing:

 This project is a Python-based application that demonstrates data collection, processing, and storage of YouTube channel information, video details, and comments into a MySQL database. The application interacts with the YouTube Data API to fetch data and utilizes SQL (via SQLAlchemy) for database operations. The user interface is built using Streamlit, allowing users to input a YouTube channel name, retrieve relevant data, push it to the database, and execute SQL queries for analysis.




Features:

  Channel Information Retrieval: Fetches basic details about a YouTube channel based on its name, including subscriber count, video count, and description.
Video Details Extraction: Retrieves detailed information about videos (e.g., title, views, likes, comments) from a specified channel.
Comment Retrieval: Collects comments associated with videos from the channel.
Data Storage: Stores fetched data into MySQL tables (channel_info, video_info, comment_info) for future analysis and reporting.
Streamlit Interface: Provides a user-friendly web interface for interacting with the application, displaying retrieved data, and executing SQL queries.





Using the Application:

 Enter a YouTube channel name into the text input field on the Streamlit web interface.
Click the "Push Data to Database" button to store the retrieved data in the MySQL database.
Explore the stored data and execute predefined SQL queries using the sidebar interface for insights.
Technologies Used
Python: Programming language for application development.
SQLAlchemy: Python SQL toolkit for database interaction.
MySQL: Database management system for data storage.
Google API Client: Python library for accessing the YouTube Data API.
Streamlit: Web application framework for building the user interface.


Contributions:

Contributions to this project are welcome! If you have suggestions, feature requests, or improvements, please feel free to submit a pull request.
