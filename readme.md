## Background
Wikiglass is a learning analytic tool for visualizing the statistics and timelines of collaborative Wikis built by secondary school students during their group project in inquiry-based learning. The tool adopts a modular structure for the flexibility of reuse with different data sources. The client side is built with the Model-View-Controller framework and the AngularJS library whereas the server side manages the database and data sources.


## Functionality
#### Data Update
The data on Wikglass is updated in a regular interval. A task scheduler in the server side is used to retrieve the newest version of the projects in the source side and updates the databases accordingly. Once the new data are extracted and processed, the visualizations will be automatically updated on the client side.
#### Weekly Email Summary
At the end of every week, teachers will receive emails summarizing the progress of the groups and students they teach. In those emails, teachers can have a quick review on the performance of different groups and students, and also are reminded to logon Wikiglass for more detailed information.


## Document

[Wikiglass-Service-Platform](https://www.showdoc.cc/12459) shows detailed implementation steps. 
