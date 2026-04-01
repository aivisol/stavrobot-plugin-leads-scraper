#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["requests"]
# ///

import json
import sys
import os
import time
import requests

ACTOR_ID = "pipelinelabs~lead-scraper-apollo-zoominfo-lusha-ppe"
APIFY_BASE = "https://api.apify.com/v2"

# Exhaustive list of titles accepted by the actor's input schema
ALLOWED_TITLES = {
    "Director", "Manager", "Founder", "General Manager", "Consultant",
    "Chief Executive Officer", "Co-Founder", "Account Manager",
    "Chief Financial Officer", "Human Resources Manager",
    "Director Of Marketing", "Executive Director", "Executive Assistant",
    "Administrative Assistant", "Director Of Human Resources", "Associate",
    "Chief Operating Officer", "HR Manager", "Account Executive",
    "Business Development Manager", "Director Of Operations", "Controller",
    "Chief Technology Officer", "Chief Information Officer", "Founder & CEO",
    "Attorney", "IT Manager", "Assistant Manager", "Engineer",
    "Business Analyst", "Accountant", "Chief Marketing Officer",
    "Creative Director", "Director Of Sales", "Graphic Designer", "Analyst",
    "Human Resources Director", "Founder And CEO",
    "Director, Information Technology", "Digital Marketing Manager",
    "Business Owner", "Assistant Professor", "Branch Manager", "HR Director",
    "Administrator", "Customer Service Representative", "HR Business Partner",
    "Co Founder", "Designer", "Intern", "Lecturer", "Architect",
    "Director Of Information Technology", "Information Technology Manager",
    "Co-Founder & CEO", "Co-Owner", "Director, Human Resources",
    "Business Development", "IT Director", "Associate Professor",
    "Finance Manager", "Director Of Business Development", "Developer",
    "Business Manager", "Director Of Engineering", "Human Resources",
    "Manager, Information Technology", "Customer Service",
    "Key Account Manager", "Executive Vice President", "Financial Analyst",
    "HR Generalist", "Financial Advisor", "Instructor", "Engineering Manager",
    "Art Director", "Director Of Sales And Marketing", "Area Manager",
    "CEO & Founder", "Director Of Finance", "Data Analyst",
    "Associate Director", "Accounting Manager", "Docente",
    "Customer Service Manager", "IT Specialist", "Account Director",
    "Data Scientist", "District Manager", "Human Resources Business Partner",
    "Co-Founder And CEO", "Assistant Principal",
    "Information Technology Director", "Facilities Manager",
    "Director Human Resources", "Exec/Management (Other)",
    "Area Sales Manager", "Executive", "Human Resources Generalist",
    "Cashier", "Design Engineer", "CEO & Co-Founder", "IT Project Manager",
    "Electrical Engineer", "Finance Director", "Head Of Marketing",
    "Independent Consultant", "Agent", "Brand Manager", "Buyer",
    "Financial Controller", "Broker", "Human Resource Manager",
    "Adjunct Professor", "Founder, CEO", "Customer Success Manager",
    "Artist", "Chairman", "Graduate Student", "CEO And Founder",
    "Director Of IT", "Educator", "Founder/CEO", "IT Consultant",
    "HR Coordinator", "Co Owner", "Lawyer", "Chief Human Resources Officer",
    "Dentist", "Editor", "Legal Assistant", "Director Of Technology",
    "Interior Designer", "Chief Operations Officer",
    "Business Development Executive", "HR Specialist", "Devops",
    "Community Manager", "Civil Engineer", "Attorney At Law",
    "Associate Consultant", "CEO And Co-Founder", "Electrician",
    "General Counsel", "District Sales Manager",
    "Director Of Product Management", "Assistant", "Driver", "Auditor",
    "Director, Marketing", "Business Consultant", "Assistant Vice President",
    "Digital Marketing Specialist", "Deputy Manager",
    "Human Resources Coordinator", "English Teacher", "Board Member",
    "IT Analyst", "Insurance Agent", "Founding Partner", "Event Manager",
    "Director Of Development", "Co-Founder & CTO", "Auxiliar Administrativo",
    "Database Administrator", "Admin", "Graduate Research Assistant",
    "Associate Attorney", "Chief Information Security Officer",
    "Director Of HR", "Chief Engineer", "Communications Manager",
    "Construction Manager", "Coordinator", "Director Of Communications",
    "Estimator", "Corporate Recruiter", "Business Development Director",
    "Enterprise Architect", "Case Manager", "Bookkeeper",
    "Chief Revenue Officer", "Analista", "Assistente Administrativo",
    "Bartender", "Advisor", "Development Manager", "Co-Founder, CEO",
    "Human Resources Specialist", "Broker Associate", "Doctor",
    "Assistant Director", "Consultor", "CTO/Cio", "Event Coordinator",
    "Chef", "Chief Product Officer", "Director Of Digital Marketing",
    "Application Developer", "HR Assistant", "HR Executive", "Directeur",
    "Executive Administrative Assistant", "Captain", "Licensed Realtor",
    "Business Development Representative", "Associate Broker",
    "Director Of Sales & Marketing", "Commercial Manager", "HR Consultant",
    "Management Trainee", "Finance", "Flight Attendant", "Lead Engineer",
    "Director Of Marketing And Communications", "Manager, Human Resources",
    "Assistant Project Manager", "Application Engineer", "Logistics Manager",
    "Assistant General Manager", "Lead Software Engineer", "Employee",
    "Founder And President", "Independent Distributor",
    "Director Of Recruiting", "CEO/Founder", "Associate Creative Director",
    "Assistant Store Manager", "Barista", "Director Of Product Marketing",
    "Corporate Controller", "Director Of Talent Acquisition",
    "Administrativo", "Assistant Controller", "Legal Secretary", "Author",
    "Commercial Director", "Chief People Officer",
    "Inside Sales Representative", "Devops Engineer",
    "Co-Founder And CTO", "Broker/Owner", "Advogado", "Field Engineer",
    "Maintenance Manager", "Clerk", "Field Service Engineer", "Cofounder",
    "Human Resources Assistant", "Executive Chef", "IT Administrator",
    "General Sales Manager", "Director, Business Development",
    "Franchise Owner", "Customer Service Supervisor", "Adjunct Faculty",
    "Benefits Manager", "Inside Sales", "Abogado", "Java Developer",
    "Head Of Product", "Management Consultant", "Contracts Manager",
    "Freelance Writer", "CEO/President/Owner", "Journalist",
    "Associate Software Engineer", "Head Of HR", "Internal Auditor",
    "Head Of Information Technology", "Founder & President", "Accounting",
    "Freelancer", "Front Office Manager", "Entrepreneur", "HR Administrator",
    "Graduate Teaching Assistant", "Director Of Sales Operations", "Diretor",
    "Data Engineer", "Librarian", "Facility Manager", "Administration",
    "IT Architect", "Legal Counsel", "Maintenance Supervisor",
    "Head Of Operations", "Founder / CEO", "Chief Strategy Officer",
    "Communications Director", "Development Director",
    "Content Marketing Manager", "Internship", "Counselor",
    "Assistant Superintendent", "Business Systems Analyst", "Design Director",
    "CEO/President", "Manager, Marketing", "Coach",
    "Freelance Graphic Designer", "Lead Developer", "Associate Manager",
    "Android Developer", "IT Department Manager", "IT Engineer",
    "Chiropractor", "Credit Analyst", "Independent Business Owner",
    "Adjunct Instructor", "Head Of Human Resources", "Brand Ambassador",
    "Copywriter", "Chairman & CEO", "Email Marketing Manager",
    "Frontend Developer", "Human Resource Director", "Client Services Manager",
    "IT Support Specialist", "Contract Manager", "Impiegato", "CEO, Founder",
    "Chief Medical Officer", "Banker", "Director Information Technology",
    "Director Of Product", "Director, Product Management", "Country Manager",
    "Financial Consultant", "Administrador", "Executive Assistant To CEO",
    "Advogada", "Field Marketing Manager", "Business Intelligence Analyst",
    "Director Marketing", "Loan Officer", "Freelance Photographer", "Actor",
    "Chef De Projet", "Foreman", "Information Technology Project Manager",
    "Graduate Assistant", "Inside Sales Manager", "Department Manager",
    "HR Officer", "Account Coordinator", "Deputy Director",
    "Director Of Facilities", "Executive Recruiter", "IT Technician",
    "CEO, Co-Founder", "Full Stack Developer", "CEO / Founder", "Counsel",
    "Logistics Coordinator", "Founder And Chief Executive Officer",
    "Chairman And CEO", "Administrative Coordinator",
    "Director Business Development", "Category Manager", "Data Architect",
    "Information Technology", "Head Of Sales",
    "Chief Information Officer (Cio)", "IT Recruiter",
    "Information Security Analyst", "Associate General Counsel", "Inspector",
    "Admin Assistant", "Dispatcher", "Contractor", "Design Manager",
    "Ecommerce Manager", "Chief Technical Officer",
    "Field Service Technician", "Executive Secretary", "Co-Founder, CTO",
    "Director, Talent Acquisition", "Accounting Assistant", "Director, IT",
    "Account Supervisor", "Human Resources Administrator", "Faculty",
    "Administrative Officer", "Front End Developer", "Content Manager",
    "Freelance", "Maintenance Technician", "Business Development Specialist",
    "Business Development Consultant", "Communications Specialist",
    "Director, Product Marketing", "Client Manager", "Compliance Officer",
    "Executive Producer", "Customer Service Specialist",
    "Certified Personal Trainer", "Human Resources Executive",
    "Chief Executive", "HR Advisor", "Compliance Manager", "Head Of IT",
    "IT Business Analyst", "Homemaker", "Events Manager", "Fleet Manager",
    "CEO & President", "Carpenter", "HR Recruiter",
    "Director, Digital Marketing", "Laboratory Technician",
    "Associate Product Manager", "Director Product Management",
    "Independent Contractor", "Accounts Payable", "Digital Marketing Director",
    "Instructional Designer", "Digital Project Manager", "Audit Manager",
    "Estudante", "Credit Manager", "Eigenaar", "Business Developer",
    "Head Of Business Development", "Avvocato", "Chief Administrative Officer",
    "Asset Manager", "Accounts Payable Specialist", "Chief Compliance Officer",
    "Empleado", "Digital Marketing Executive", "Account Representative",
    "Campaign Manager", "Director, Engineering", "Engagement Manager",
    "Management", "Delivery Manager", "Manager Human Resources", "Cook",
    "Director Of Product Development", "Information Technology Specialist",
    "Chief Of Staff", "Associate Vice President", "Company Director",
    "Chief Technology Officer (CTO)", "Digital Marketing Consultant",
    "Firefighter", "Business Operations Manager", "Crew Member",
    "Director - Human Resources", "Caregiver", "Customer Experience Manager",
    "Financial Accountant", "Customer Service Rep", "Bank Teller",
    "IT Operations Manager", "Management Accountant", "Digital Marketing",
    "Investigator", "Enterprise Account Executive", "Logistics",
    "Deputy General Manager", "Freelance Designer", "Economist",
    "Digital Marketing Coordinator", "Co-Founder & COO", "Chief Architect",
    "Learning And Development Manager", "Director General", "Distributor",
    "Associate Marketing Manager", "Abogada", "Assistant General Counsel",
    "Machine Operator", "Delivery Driver", "Comercial", "Chemist", "Hostess",
    "Lead Consultant", "Director Of Training", "Financial Representative",
    "Maintenance", "Audit Associate", "Housewife", "Assistant Accountant",
    "Financial Manager", "Maintenance Engineer", "Contract Administrator",
    "First Officer", "Director Of Marketing Communications", "Comptable",
    "Finance Officer", "Financial Planner", "Automation Engineer",
    "Administrativa", "Estudiante", "Accounts Manager",
    "Customer Service Associate", "Investment Banking Analyst", "Director HR",
}


def parse_csv(value: str) -> list[str]:
    """Parse a comma-separated string into a trimmed list."""
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def load_config() -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "..", "config.json")
    with open(config_path) as f:
        return json.load(f)


def build_actor_input(params: dict) -> dict:
    actor_input = {}

    str_array_fields = {
        "company_industry_includes": "companyIndustryIncludes",
        "company_keyword_includes": "companyKeywordIncludes",
        "company_location_country_excludes": "companyLocationCountryExcludes",
        "person_location_country_includes": "personLocationCountryIncludes",
        "person_location_country_excludes": "personLocationCountryExcludes",
        "person_location_state_includes": "personLocationStateIncludes",
        "person_location_state_excludes": "personLocationStateExcludes",
        "person_location_city_includes": "personLocationCityIncludes",
        "person_location_city_excludes": "personLocationCityExcludes",
        "person_title_includes": "personTitleIncludes",
    }

    for param_key, actor_key in str_array_fields.items():
        if param_key in params and params[param_key]:
            values = parse_csv(params[param_key])
            if param_key == "person_title_includes":
                invalid = [v for v in values if v not in ALLOWED_TITLES]
                if invalid:
                    print(json.dumps({
                        "error": f"Invalid job title(s): {invalid}. Must be exact values from the allowed list.",
                        "allowed_titles": sorted(ALLOWED_TITLES),
                    }))
                    sys.exit(1)
            actor_input[actor_key] = values

    bool_fields = {
        "has_email": ("hasEmail", True),
        "has_phone": ("hasPhone", False),
        "include_similar_titles": ("includeSimilarTitles", False),
        "reset_saved_progress": ("resetSavedProgress", False),
    }

    for param_key, (actor_key, default) in bool_fields.items():
        actor_input[actor_key] = params.get(param_key, default)

    actor_input["totalResults"] = params.get("total_results", 100)

    return actor_input


def wait_for_run(run_id: str, token: str, poll_interval: int = 10, max_wait: int = 600) -> dict:
    """Poll until the actor run finishes or max_wait seconds elapse."""
    url = f"{APIFY_BASE}/actor-runs/{run_id}"
    deadline = time.time() + max_wait

    while time.time() < deadline:
        resp = requests.get(url, params={"token": token}, timeout=30)
        resp.raise_for_status()
        run = resp.json()["data"]
        status = run["status"]

        if status == "SUCCEEDED":
            return run
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Actor run ended with status: {status}")

        time.sleep(poll_interval)

    raise TimeoutError(f"Actor run {run_id} did not finish within {max_wait} seconds")


def fetch_dataset(dataset_id: str, token: str, limit: int) -> list:
    url = f"{APIFY_BASE}/datasets/{dataset_id}/items"
    resp = requests.get(
        url,
        params={"token": token, "limit": limit, "clean": "true"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def main():
    try:
        config = load_config()
    except FileNotFoundError:
        print(json.dumps({"error": "config.json not found — please configure apify_api_key"}))
        sys.exit(1)

    api_token = config.get("apify_api_key", "").strip()
    if not api_token:
        print(json.dumps({"error": "apify_api_key is missing or empty in config.json"}))
        sys.exit(1)

    params = json.load(sys.stdin)
    actor_input = build_actor_input(params)
    total_results = actor_input["totalResults"]

    # Start the actor run (don't wait inline — use async polling instead)
    run_url = f"{APIFY_BASE}/acts/{ACTOR_ID}/runs"
    try:
        resp = requests.post(
            run_url,
            params={"token": api_token},
            json=actor_input,
            timeout=60,
        )
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(json.dumps({"error": f"Failed to start actor run: {e}", "response": resp.text}))
        sys.exit(1)

    run_data = resp.json()["data"]
    run_id = run_data["id"]

    # Poll until complete
    try:
        finished_run = wait_for_run(run_id, api_token)
    except (RuntimeError, TimeoutError) as e:
        print(json.dumps({"error": str(e), "run_id": run_id}))
        sys.exit(1)

    dataset_id = finished_run["defaultDatasetId"]

    try:
        leads = fetch_dataset(dataset_id, api_token, total_results)
    except requests.HTTPError as e:
        print(json.dumps({"error": f"Failed to fetch results: {e}", "run_id": run_id}))
        sys.exit(1)

    print(json.dumps({
        "status": "success",
        "total_leads": len(leads),
        "run_id": run_id,
        "dataset_id": dataset_id,
        "leads": leads,
    }))


if __name__ == "__main__":
    main()
