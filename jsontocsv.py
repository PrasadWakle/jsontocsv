import json
import pandas


def read_json(filename: list) -> dict:

    try:
        with open(filename, "r") as f:
            data = json.loads(f.read())
    except:
        raise Exception(f"Reading {filename} file encountered an error")

    return data


def create_dataframe(data: str) -> pandas.DataFrame:

    # Declare an empty dataframe to append records
    dataframe = pandas.DataFrame()

    # Looping through each record
    for d in data['workers']:

        # Normalize the column levels
        name_details = pandas.json_normalize(d, record_path=['nameDetails'])
        contact_details = pandas.json_normalize(d, record_path=['phoneContactDetails'])
        email = pandas.json_normalize(d, record_path=['emailContactDetails'],meta=[['employmentSummary','createAccessDate'],['employmentSummary','createAccessTime'],['employmentSummary','mostRecentHireDate']])
        record = pandas.json_normalize(d, record_path=['addressDetails'])
        job_details = pandas.json_normalize(d, record_path=['jobDetails'],meta=['workerIdentifier'])
        
        new = pandas.concat([name_details,contact_details,email,record,job_details],axis=1,join='inner')

        # Append it to the dataframe
        dataframe = dataframe.append(new, ignore_index=True)
    

    return dataframe


def main():
    # Read the JSON file as python dictionary
    data = read_json(filename="work.json")

    # Generate the dataframe for the array items in
    # details key
    dataframe = create_dataframe(data=data['workerDataResponse'])

    # Renaming columns of the dataframe
    dataframe.columns.to_list()

    dataframe.rename(columns={
    	"employmentSummary.createAccessDate": "accessDate",
    	"employmentSummary.createAccessTime": "accessTime",
    	"employmentSummary.mostRecentHireDate": "mostRecentHireDate",
    	"employmentJobProfileDetails.jobProfileIdentifier": "jobProfileIdentifier",
    	"jobGovernanceRoleDetails.functionalManagerWorkerIdentifier": "functionalManagerWorkerIdentifier",
        "organizationDetails.companyOrganizationIdentifier":"companyOrganizationIdentifier"
    }, inplace=True)

    dataframe.columns.to_list()

    # Convert dataframe to CSV
    dataframe.to_csv("emp_data.csv", index=False)


if __name__ == '__main__':
    main()
