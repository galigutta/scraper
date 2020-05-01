from chalice import Chalice
import requests, pytz
import boto3, botocore
import pandas as pd
import datetime,time,os
import pandas_market_calendars as mcal

app = Chalice(app_name='scraper')
prod_dict={
        'rb':'429',
        'ho':'426',
        'cl':'425',
        'ng':'444'
        }
utc = pytz.utc
utc_dt = datetime.datetime.today()
eastern = pytz.timezone('US/Eastern')
loc_dt = utc_dt.astimezone(eastern)
fmt = '%Y%m%d'
rep_date = loc_dt.strftime(fmt)
aws_bkt='cme-oi'
#print(rep_date) #debug

@app.lambda_function()
def index(event,context):
    print('printing event from lambda...')
    print(event)
    main_fun()
    return {'hello':'Success'}

def get_xls_from_cme(rep_date,ticker):
    url = 'https://www.cmegroup.com/CmeWS/exp/voiProductDetailsViewExport.ctl?media=xls&tradeDate='+rep_date+'&reportType=P&productId='+prod_dict[ticker]
    r = requests.get(url, allow_redirects=True)
    open('/tmp/'+rep_date+ticker+'.xls', 'wb').write(r.content)
#    s3 = boto3.client('s3')
#    with open('/tmp/'+rep_date+'.xls', 'rb') as f:
#        s3.upload_fileobj(f, 'tsla-oi', rep_date+'.xls')
    return url


@app.schedule('cron(23 1,3,9 ? * * *)')
def cron_handler(event):
    print('from lambda cron')
    print(event)
    main_fun()
    return {'hello':'Success'}

def get_or_create_csv(ticker):
    s3 = boto3.resource('s3')
    csv_file='consolidated_'+ticker+'_oi.csv'
    try:
        s3.Bucket(aws_bkt).download_file(csv_file,'/tmp/'+csv_file)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print('no file in s3, creating..')
            col_names=['Month', 'Globex', 'Open OutCry', 'Clear Port', 'Total Volume',
       'Block Trades', 'EFP', 'EFR', 'EFS', 'TAS', 'Deliveries', 'At Close',
       'Change', 'Trade Day']
            con = pd.DataFrame(columns = col_names)
            con.to_csv('/tmp/consolidated_'+ticker+'_oi.csv',index=False)
            dates_for_data = get_last_few_trade_dates(30)
            for rep_date in dates_for_data:
               time.sleep(1)
               get_xls_from_cme(rep_date.strftime('%Y%m%d'),ticker)
            merge_cme_files_to_csv(ticker)
        else:
            # Something else has gone wrong.
            raise
    else:
        print('file found')
        
        
def get_last_few_trade_dates(num_days):
    cme = mcal.get_calendar('CME')
    #get the last 2 trading days
    return cme.valid_days(start_date=(loc_dt+datetime.timedelta(days=-100)).strftime('%Y%m%d'),end_date=loc_dt.strftime('%Y%m%d'))[-num_days:]

def merge_cme_files_to_csv(ticker):
    trade_days=[]
    for file in os.listdir("/tmp"):
        if file.endswith(ticker+".xls"):
#            print(file)
            trade_days.append(file[:8])
    trade_days.sort()
    con=pd.read_csv('/tmp/consolidated_'+ticker+'_oi.csv')
    for td in trade_days:
        print(td)
        df = pd.read_excel(r'/tmp/'+td+ticker+'.xls',skiprows =4,header=1)
        if (df.size) > 3000: # To filer out empty files
#            print(df.size)
            #slice to first occurrence of "TOTALS"
            df=df.loc[: df[(df['Month'] == 'TOTALS')].index[0]-1, :]
            df['Trade Day']=int(td)
            df['Month']=pd.to_datetime(df['Month'],format='%b %y').dt.strftime('%Y-%m')
            con = con.query('`Trade Day` != @td')
            con = con.append(df)
#            print(df.size)
#            print(con.size,con2.size)
    con.to_csv('/tmp/consolidated_'+ticker+'_oi.csv',index=False)
    piv_tab_oi=pd.pivot(con, values='At Close',index='Month', columns='Trade Day')
    piv_tab_oi.sort_index(axis=1, ascending=False, inplace=True)
    piv_tab_oi.fillna('', inplace=True)
    piv_tab_chg=pd.pivot(con, values='Change',index='Month', columns='Trade Day')
    piv_tab_chg.sort_index(axis=1, ascending=False, inplace=True)
    piv_tab_chg.fillna('', inplace=True)
    with open("/tmp/index.html", 'a') as htmlfile:
        htmlfile.write('<br>'+ticker.upper()+' OI'+'<br>'+piv_tab_oi.to_html())
        htmlfile.write('<br>'+ticker.upper()+' OI Change'+'<br>'+piv_tab_chg.to_html())
        htmlfile.close()
        
    
def upload_to_s3(ticker):
    s3 = boto3.client('s3')
    print('uploading '+ticker)
    with open('/tmp/consolidated_'+ticker+'_oi.csv', "rb") as f:
        s3.upload_fileobj(f, aws_bkt, 'consolidated_'+ticker+'_oi.csv')

def main_fun():   
    with open("/tmp/index.html", 'w') as htmlfile:
        htmlfile.write("\nLast updated at: "+loc_dt.strftime("%Y-%m-%d %H:%M:%S")+ " EST<br>")    

    for ticker in list(prod_dict.keys()):
        get_or_create_csv(ticker) #gets csv from s3 to local
        dates_for_data = get_last_few_trade_dates(2)
        for rep_date in dates_for_data:
           time.sleep(1)
           get_xls_from_cme(rep_date.strftime('%Y%m%d'),ticker)
        merge_cme_files_to_csv(ticker)
        upload_to_s3(ticker)
        
    s3 = boto3.client('s3')
    with open("/tmp/index.html", "rb") as f:
        print('uploading html')
        s3.upload_fileobj(f, aws_bkt, "index.html",ExtraArgs={'ContentType':'text/html','ACL':'public-read'})   
        
if __name__ == "__main__": 
    print("Invoked directly")
    main_fun()
else: 
    print("External invocation")

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
