# EMRServerlessAPI
An example usage of the StartJobRun and GetJobRun APIs for EMR Serverless

This example works with an application configured to auto-start when a job is submitted. If the application is not configured this way, start the application before submitting the job by using the StartApplication API. 

All places that need to be replaced with your desired parameters (ex: jar file, entryPointConfigurations, appId, etc.) have been commented in the code. 

To build a jar from this example: 
1. Create a new Maven project
2. Modify the pom.xml to the one in this git repo. 
3. Add the App.java file to your src code. 
4. Go to Lifecycle -> package
5. The output jar file will be put in the target folder (generated by Maven). 
