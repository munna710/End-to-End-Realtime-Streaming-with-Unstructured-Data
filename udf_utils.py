import re
from datetime import datetime

def extract_file_name(file_content):
    file_content = file_content.strip()
    position = file_content.split('\n')[0]
    return position

def extract_position(file_content):
    file_content = file_content.strip()
    position = file_content.split('\n')[0]
    return position

def extract_classcode(file_content):
    try:
        classcode_match = re.search('(Class Code: )\s+(\d+)', file_content)
        classcode = classcode_match.group(2) if classcode_match else None
        return classcode
    except Exception as e:
        raise ValueError(f'Error extracting classcode: {e}')


def extract_salary(file_content):
    pass

def extract_requirements(file_content):
    pass

def extract_notes(file_content):
    pass

def extract_duties(file_content):
    pass

def extract_start_date(file_content):
    try:
        opendate_match = re.search('(Open Date: )\s+(\d+/\d+/\d+)', file_content)
        opendate = opendate_match.group(2) if opendate_match else None
        return datetime.strptime(opendate, '%m/%d/%Y')
    except Exception as e:
        raise ValueError(f'Error extracting opendate: {e}')

def extract_end_date(file_content):
    try:
        enddate_match = re.search('(End Date: )\s+(\d+/\d+/\d+)', file_content)
        enddate = enddate_match.group(2) if enddate_match else None
        return datetime.strptime(enddate, '%m/%d/%Y')
    except Exception as e:
        raise ValueError(f'Error extracting enddate: {e}')

def extract_selection(file_content):
    pass

def extract_education_length(file_content):
    pass

def extract_school_type(file_content):
    pass

def extract_experience_length(file_content):
    pass

# def extract_job_type(file_content):
#     pass

def extract_application_location(file_content):
    pass
