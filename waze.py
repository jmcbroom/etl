import sodapy, os

soda_token = os.environ['SODA_TOKEN']
soda_user = os.environ['SODA_USER']
soda_pass = os.environ['SODA_PASS']
soda_connection = sodapy.Socrata('data.detroitmi.gov', soda_token, soda_user, soda_pass, timeout=54000)

def create_files():
  params = {
    "description": "Waze specification XML file",
    "tags": ["waze", "traffic"]
  }
  with open("/home/gisteam/waze/CIFS_COD_Construction.xml", 'rb') as f:
      files = (
          {'file': ("CIFS_COD_Construction.xml", f)}
      )
      response = soda_connection.create_non_data_file(params, files)
  with open("/home/gisteam/waze/CIFS_MDOT_Construction.xml", 'rb') as f:
      files = (
          {'file': ("CIFS_MDOT_Construction.xml", f)}
      )
      response = soda_connection.create_non_data_file(params, files)
  with open("/home/gisteam/waze/CIFS_MDOT_Incident.xml", 'rb') as f:
      files = (
          {'file': ("CIFS_MDOT_Incident.xml", f)}
      )
      response = soda_connection.create_non_data_file(params, files)

def update_files():

  with open("/home/gisteam/waze/CIFS_COD_Construction.xml", 'rb') as f:
    files = (
        {'file': ("CIFS_COD_Construction.xml", f)}
    )
    response = client.replace_non_data_file("7k6j-d68u", {}, files)

  with open("/home/gisteam/waze/CIFS_MDOT_Construction.xml", 'rb') as f:
    files = (
        {'file': ("CIFS_MDOT_Construction.xml", f)}
    )
    response = client.replace_non_data_file("4865-hjyr", {}, files)

  with open("/home/gisteam/waze/CIFS_MDOT_Incident.xml", 'rb') as f:
    files = (
        {'file': ("CIFS_MDOT_Incident.xml", f)}
    )
    response = client.replace_non_data_file("aqaa-nn7j", {}, files)

if __name__ == "__main__":
  update_files()
