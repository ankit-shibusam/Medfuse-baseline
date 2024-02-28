import pandas as pd
import subprocess
import threading

# Read the CSV file into a DataFrame
df = pd.read_csv('mimic-cxr-2.0.0-metadata.csv')

# Convert 'study_id' column to numeric
df['study_id_num'] = pd.to_numeric(df['study_id'], errors='coerce')

# Group by 'subject_id' and filter rows where 'study_id' is maximum
result = df.loc[df.groupby('subject_id')['study_id_num'].idxmax()][['subject_id', 'study_id', 'dicom_id']]



def download_file(subject_id, study_id, dicom_id):
    # Execute wget command using subprocess
    username = 'kushagragarwal2443'
    password = 'x2qSYHuL.3N-GC$'
    url = 'https://physionet.org/files/mimic-cxr-jpg/2.0.0/files/p%s/p%s/s%s/%s.jpg' % (str(subject_id)[:2], subject_id, study_id, dicom_id)
    #output_path = 'files/p%s/p%s/s%s/' % (str(subject_id)[:2], subject_id, study_id)
    command = [
        'wget', '-N', '-c', '-np',
        f'--user={username}',
        f'--password={password}',
        '-P', output_path,
        url
    ]
    #print("Loading Image: %s" % url)
    subprocess.run(command)

def download_files(df):

    # Define the number of threads
    num_threads = 8

    # Download files using multiple threads
    for i in range(0, 30000, num_threads):
        # Create threads for each download task
        threads = []
        for j in range(num_threads):
            if i + j < len(df):
                subject_id = df.iloc[i + j]['subject_id']
                study_id = df.iloc[i + j]['study_id']
                dicom_id = df.iloc[i + j]['dicom_id']

                thread = threading.Thread(target=download_file, args=(subject_id, study_id, dicom_id))
                threads.append(thread)
                thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

download_files(result)
