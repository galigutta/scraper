from chalice import Chalice
import requests, pytz
import boto3
import datetime
#import pandas

app = Chalice(app_name='scraper')
pid=str(429)
utc = pytz.utc
utc_dt = datetime.datetime.today()
eastern = pytz.timezone('US/Eastern')
loc_dt = utc_dt.astimezone(eastern)
fmt = '%Y%m%d'
rep_date = loc_dt.strftime(fmt)
rep_date='20200402'
#print(rep_date) #debug

@app.lambda_function()
def index(event,context):
    print('printing event...')
    print(event)
    get_xls_from_cme(rep_date,pid)
    return {'hello':'Success' }

def get_xls_from_cme(rep_date,pid):
	url = 'https://www.cmegroup.com/CmeWS/exp/voiProductDetailsViewExport.ctl?media=xls&tradeDate='+rep_date+'&reportType=P&productId='+pid
	r = requests.get(url, allow_redirects=True)
	open('/tmp/'+rep_date+'.xls', 'wb').write(r.content)
	s3 = boto3.client('s3')
	with open('/tmp/'+rep_date+'.xls', 'rb') as f:
		s3.upload_fileobj(f, 'tsla-oi', rep_date+'.xls')
	return url

@app.schedule('cron(40 2 ? * * *)')
def cron_handler(event):
    print('from cron')
    print(event,"context")
    index()
	


# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#
