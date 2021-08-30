# qlik-python-project

## REQUIREMENTS

- Qlik Sense February 2018+
    - *See how to setup Analytic Connections within QlikView [here](https://help.qlik.com/en-US/qlikview/November2017/Subsystems/Client/Content/Analytic_connections.htm)*
- Python 3.5.3 64 bit or later. Download [here](https://www.python.org/downloads/). Suggested path to download: C:\python.
- Python Libraries: grpcio, hyper

## LAYOUT

- [Prepare your Project Directory](#prepare-your-project-directory)
- [Install Python Libraries and Required Software](#install-python-libraries-and-required-software)
- [Setup an AAI Connection in the QMC](#setup-an-aai-connection-in-the-qmc)
- [Copy the Package Contents and Import Examples](#copy-the-package-contents-and-import-examples)
- [Prepare And Start Services](#prepare-and-start-services)
- [Leverage Python Extension from within Qlik Sense](#leverage-python-extension-from-within-qlik-sense)
- [Configure your SSE as a Windows Service](#configure-your-sse-as-a-windows-service)

 
## PREPARE YOUR PROJECT DIRECTORY
1. Open a command prompt
2. Make a new project folder called DefendantMatch, where all of our projects will live that leverage the DefendantMatch environment that we’ve created. Let’s place it under ‘C:\python’.
3. We now want to leverage our environment. If you are not already in your environment, enter it by executing:

```shell
$ workon DefendantMatch
```

4. Now, ensuring you are in the ‘DefendantMatch’ folder that you created. Execute the following commands to create and navigate into your project’s folder structure:
```
$ cd DefendantMatch
$ mkdir DefendantMatch
$ cd DefendantMatch
```
5. We have now set the stage for our environment. To navigate back into this project in the future, simply execute:
```shell
$ workon DefendantMatch
```

This will take you back into the environment with the default directory that we set above. To change the
directory for future projects within the same environment, change your directory to the desired path and reset
the working directory with ‘setprojectdir .’


## INSTALL PYTHON LIBRARIES AND REQUIRED SOFTWARE

1. Open a command prompt or continue in your current command prompt, ensuring that you are currently within the environment—you will see (DefendantMatch) preceding the directory if so. If you are not, execute:
```shell
$ workon DefendantMatch
```
2. Execute the following commands. 

```shell
$ pip install grpcio
$ python -m pip install grpcio-tools
$ python setup.py install
$ pip install hyper
```

## SET UP AN AAI CONNECTION IN THE QMC

1. Navigate to the QMC and select ‘Analytic connections’
2. Fill in the **Name**, **Host**, and **Port** parameters -- these are mandatory.
    - **Name** is the alias for the analytic connection. For the example qvf to work without modifications, name it 'PythonTranslate'
    - **Host** is the location of where the service is running. If you installed this locally, you can use 'localhost'
    - **Port** is the target port in which the service is running. This module is setup to run on 50055, however that can be easily modified by searching for ‘-port’ in the ‘\_\_main\_\_.py’ file and changing the ‘default’ parameter to an available port.
3. Click ‘Apply’, and you’ve now created a new analytics connection.

## COPY THE PACKAGE CONTENTS AND IMPORT EXAMPLES

1. To start the defendant match service and app, clone the repository to the ‘..\python\DefendantMatch’ location. 

## PREPARE AND START SERVICES

1. At this point the setup is complete, and we now need to start the python extension service. To do so, navigate back to the command prompt. Please make sure that you are inside of the environment.
2. Once at the command prompt and within your environment, execute (note two underscores on each side):
```shell
$ python __main__.py
```
3. We now need to restart the Qlik Sense engine service so that it can register the new AAI service. To do so,
    navigate to windows Services and restart the ‘Qlik Sense Engine Service’
4. You should now see in the command prompt that the Qlik Sense Engine has registered the defendant match
    function from the extension service over port 50055, or whichever port you’ve chosen to leverage.


## LEVERAGE PYTHON DEFENDANT MATCH FROM WITHIN SENSE

1. The *DefendantMatch()* function leverages our script and accepts two mandatory arguments:
    - *DefendantName (string)*
    - *AlertUniqueID (string)*
2. Example function calls:
	
    *match defendants to clients*:
    ``` Python.DefendantMatch(DefendantTable{"DefendantName", "AlertUniqueID"}) ``` 
    


```
//LIB CONNECT TO 'LitAlerts_REST_ClientNameSearch (piper_jp41325)';
//LIB CONNECT TO 'REST_ClientNameSearch (inoutsource_jpierantozzi)';

 


[Client_Search_Results_Prime]:
//LOAD * FROM 'lib://Attachments Folder (piper_as40298)/LitAlertsMatchResults.qvd'(qvd);
LOAD * FROM 'lib://LitAlerts (inoutsource_jpierantozzi)/demo/LitAlertsMatchResults.qvd'(qvd);

 

[TokensToIgnore]:
 LOAD * Inline [TokenString
 'The'];

 

 LOAD * Inline [TokenString
 'A Delaware Corporation'];

 

[DoingBusinessAsMapping]:
MAPPING LOAD * INLINE ['TokenToFind', 'TokenToReplace'
'doing business as', ';'
'd/b/a', ';'
' dba ', ';'];

 

[CNS_Alerts]:
// Load * From 'lib://Attachments Folder (piper_as40298)/CNS Alerts.qvd'(qvd)
// where IsNull([MatchRunDate]) OR Len(Trim(MatchRunDate)) = 0; 
LOAD
    AlertUniqueID,
    Source,
    "fileName",
    fileTimestamp,
    AlertUploadDate,
    Plaintiffs,
    Defendants,
    Summary,
    CourtName,
    "Filing Date",
    CaseNumber,
    Judge,
    "Plaintiff Lawyer",
    "Plaintiff Lawyer Firms",
    "Defendant Lawyers",
    "Defendant Lawyer Firms",
    City,
    State,
    NOS,
    "Download Link",
    MatchRunDate,
    PossibleDuplicate,
    DuplicateAlertID,
    ComplaintID,
    BatchID,
    CaseString,
    NormCourtText,
    NormCourtName,
    DefendantCount,
    PlaintiffCount,
    StringSum,
    PlaintiffLDString,
    DefendantLDString
FROM [lib://DB Shares folder (inoutsource_aspivey) (inoutsource_zbeauchemin)/NCFI/CNS Alerts.qvd]
(qvd);

 

//  trace('Starting Find CNS matches');
Set defendantNum = 0;
// let CNSrows = NoOfRows('CNS_Alerts');
let ignoreTokensRows = NoOfRows('TokensToIgnore');

 

DefendantTable:
NoConcatenate 
LOAD CaseNumber,
     SubField(Defendants, ';') as DefendantName,
     'CNS' & PurgeChar(PurgeChar(PurgeChar(Text(now(1)),chr(32)), chr(58)), chr(47)) & Text(rowno()) as AlertUniqueID,
     Now() as POCUploadDate,
     Now() as SearchMatchRunDate,
//     Field2 as ClientName,
//     Field3 as ClientNumber,
     'CNS' as AlertType,
     0 as _score
     //PurgeChar(PurgeChar(PurgeChar(PurgeChar('$(curCNSUniqueID)' & '$(curDefendant)', ' '), '/'), ':'), ',') as DefendantUniqueID 
RESIDENT CNS_Alerts;

 

FinalMatchResults:
NoConcatenate
LOAD //CaseNumber,
     Field1 as DefendantName,
     Field2 as AlertUniqueID,
     //POCUploadDate,
     //SearchMatchRunDate,
     Field3 as ClientName,
     Field4 as ClientNumber,
     Field5 as AverageRatio
     //AlertType,
     //_score
EXTENSION Python.DefendantMatch(DefendantTable{"DefendantName", "AlertUniqueID"});

 

Drop table DefendantTable;
Drop table CNS_Alerts;
drop table Client_Search_Results_Prime;
drop table TokensToIgnore;

 

exit script;
```

## CONFIGURE YOUR SSE AS A WINDOWS SERVICE

Using NSSM we can turn a Python SSE into a Windows Service. You will want to run your SSEs as services so that they startup automatically and run in the background.
1. The **Path** needs to be the location of your desired Python executable. You can find that under 'C:\python\python.exe'.
2. the **Startup directory** needs to be the parent folder of the extension service. The folder needs to contain the '_\_main\_\_.py' file.
3. The **Arguments** parameter is the '\_\_main\_\_.py' file.

**Example:**

![nssmexample](https://user-images.githubusercontent.com/85905023/131260580-3b4c6472-697a-412c-9f3b-329998186e14.PNG)
