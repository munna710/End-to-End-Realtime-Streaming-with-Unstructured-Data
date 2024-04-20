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
    try:
        salary_pattern = r'\$(\d{1,3}(?:,\d{3})+).+?to.+\$(\d{1,3}(?:,\d{3})+)(?:\s+and\s+\$(\d{1,3}(?:,\d{3})+)\s+to\s+)?'
        salary_match = re.search(salary_pattern, file_content)
        if salary_match:
            salary_start = float(salary_match.group(1).replace(',', ''))
            salary_end = float(salary_match.group(3).replace(',', '')) if salary_match.group(3) else float(salary_match.group(2).replace(',', ''))
        else:
            salary_start, salary_end = None, None
        return salary_start, salary_end
    except Exception as e:
        raise ValueError(f'Error extracting salary: {str(e)}')

def extract_requirements(file_content):
    try:
        req_match = re.search('(REQUIREMENTS/MINIMUM QUALIFICATIONS)\s+(.+?)\s+(NOTES)', file_content, re.DOTALL)
        req = req_match.group(2) if req_match else None
        return req.split("\r")  # Windows-style line endings
    except Exception as e:
        raise ValueError(f'Error extracting requirements: {e}')

def extract_notes(file_content):
    try:
        notes_match = re.search('(NOTES)\s+(.+?)\s+(DUTIES)', file_content, re.DOTALL)
        notes = notes_match.group(2) if notes_match else None
        return notes.split("\r")  # Windows-style line endings
    except Exception as e:
        raise ValueError(f'Error extracting notes: {e}')

def extract_duties(file_content):
    try:
        duties_match = re.search('(DUTIES)\s+(.+?)\s+(REQ)', file_content, re.DOTALL)
        duties = duties_match.group(2) if duties_match else None
        return duties.split("\r")  # Windows-style line endings
    except Exception as e:
        raise ValueError(f'Error extracting duties: {e}')

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
    try:
        selection_pattern = r'([A-Z][a-z]+)(\s\s)'
        selection_match = re.findall(selection_pattern, file_content)
        selections = [z[0] for z in selection_match] if selection_match else None
        return selections
    except Exception as e:
        raise ValueError(f'Error extracting selection: {str(e)}')

def extract_education_length(file_content):
    try:
        education_pattern = r'(\d+)(\s+years?)(\s+education)'
        education_match = re.search(education_pattern, file_content)
        education_length = education_match.group(1) if education_match else None
        return education_length
    except Exception as e:
        raise ValueError(f'Error extracting education_length: {str(e)}')

# def extract_school_type(file_content):
#     pass

def extract_experience_length(file_content):
    try:
        experience_pattern = r'(\d+)(\s+years?)(\s+experience)'
        experience_match = re.search(experience_pattern, file_content)
        experience_length = experience_match.group(1) if experience_match else None
        return experience_length
    except Exception as e:
        raise ValueError(f'Error extracting experience_length: {str(e)}')

# def extract_job_type(file_content):
#     pass

def extract_application_location(file_content):
    try:
        location_pattern = r'(Application\s+must\s+be\s+received\s+by\s+5:00\s+p.m.\s+on\s+)(\w+\s+\d+,\s+\d{4})'
        location_match = re.search(location_pattern, file_content)
        location = location_match.group(2) if location_match else None
        return location
    except Exception as e:
        raise ValueError(f'Error extracting application_location: {str(e)}')
