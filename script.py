from googleapiclient.discovery import build
import googleapiclient.discovery
import os
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
import gspread
from sqlalchemy import create_engine
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import dash
from dash import dcc
import plotly.graph_objs as go
from dash.dependencies import Input, Output
import plotly.express as px
from dash import html
import dash_bootstrap_components as dbc
import datetime as dt

def get_basic_info():
    API_KEY = 'AIzaSyAxfII0tgeA4QMF0NlTGQGPU0An_h_eqTQ'
    api_service_name = "youtube"
    api_version = "v3"
    channelId = "UCBC5Nwxcp1f8-bEdPOK53ug"
    service = googleapiclient.discovery.build(api_service_name, api_version, developerKey=API_KEY)
    response_items = service.search().list(
        channelId=channelId,
        part="snippet",
        type='video',
        maxResults="50",
    ).execute()

    video_ids = []
    next_page_token = response_items.get('nextPageToken')
    status = True

    while status:
        if next_page_token is None:
            status = False
        response_items = service.search().list(
            channelId=channelId,
            part="snippet",
            type='video',
            maxResults="50",
            pageToken=next_page_token
        ).execute()

        for i in range(len(response_items['items'])):
            video_ids.append(response_items['items'][i]['id']['videoId'])

        next_page_token = response_items.get('nextPageToken')
    all_video = []
    for i in range(0, len(video_ids), 50):
        request = service.videos().list(
            part="snippet,statistics",
            id=','.join(video_ids[i: i + 50])
        )
        response = request.execute()
        for video in response['items']:
            video_sum = dict(video_id=video['id'],
                             title=video['snippet']['title'],
                             published_at=video['snippet']['publishedAt'])
            all_video.append(video_sum)
    return all_video


def get_advanced_info():
    api_service_name = 'youtubeAnalytics'
    version = 'v2'
    credentials = cred_saves()
    youtube_analytics = googleapiclient.discovery.build(api_service_name, version, credentials=credentials)
    request_videos_info = youtube_analytics.reports().query(
        dimensions="video",
        endDate="2022-11-13",
        ids="channel==MINE",
        maxResults=200,
        metrics="estimatedMinutesWatched,views,likes,subscribersGained,comments,averageViewDuration,cardClicks,cardTeaserClicks,cardImpressions",
        sort="-estimatedMinutesWatched",
        startDate="2021-10-12"
    )
    response_videos_info = request_videos_info.execute()
    video_details = []
    for i in range(len(response_videos_info['rows'])):
        video_metrics = dict(video_id=response_videos_info['rows'][i][0],
                            estimatedMinutesWatched=response_videos_info['rows'][i][1],
                            views=response_videos_info['rows'][i][2],
                            likes=response_videos_info['rows'][i][3],
                            subscribersGained=response_videos_info['rows'][i][4],
                            comments=response_videos_info['rows'][i][5],
                            averageViewDuration=response_videos_info['rows'][i][6],
                            cardClicks=response_videos_info['rows'][i][7],
                            cardTeaserClicks=response_videos_info['rows'][i][8],
                            cardImpressions=response_videos_info['rows'][i][9])
        video_details.append(video_metrics)

    time_based_info = youtube_analytics.reports().query(
        dimensions="day",
        endDate="2022-11-13",
        ids="channel==MINE",
        maxResults=200,
        metrics="estimatedMinutesWatched,views,likes,subscribersGained,comments,averageViewDuration,cardClicks,cardTeaserClicks,cardImpressions",
        sort="-estimatedMinutesWatched",
        startDate="2021-10-12"
    )
    response_time_based_info = time_based_info.execute()
    video_details_by_time = []
    for i in range(len(response_time_based_info['rows'])):
        time_based_video_metrics = dict(date=response_time_based_info['rows'][i][0],
                            estimatedMinutesWatched=response_time_based_info['rows'][i][1],
                            views=response_time_based_info['rows'][i][2],
                            likes=response_time_based_info['rows'][i][3],
                            subscribersGained=response_time_based_info['rows'][i][4],
                            comments=response_time_based_info['rows'][i][5],
                            averageViewDuration=response_time_based_info['rows'][i][6],
                            cardClicks=response_time_based_info['rows'][i][7],
                            cardTeaserClicks=response_time_based_info['rows'][i][8],
                            cardImpressions=response_time_based_info['rows'][i][9])
        video_details_by_time.append(time_based_video_metrics)
    return [video_details, video_details_by_time]


def cred_saves():
    scopes = ['https://www.googleapis.com/auth/youtube',
              'https://www.googleapis.com/auth/yt-analytics.readonly',
              'https://www.googleapis.com/auth/youtube.readonly',
              'https://www.googleapis.com/auth/yt-analytics-monetary.readonly',
              'https://www.googleapis.com/auth/youtubepartner'
    ]
    path = 'token.json'
    creds = None
    MY_OAUTH_TOKEN = 'client_secret_483785681501.json'
    if os.path.exists(path):
        creds = Credentials.from_authorized_user_file(path, scopes)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(MY_OAUTH_TOKEN, scopes)
            creds = flow.run_local_server(port=5000)
    # Save the credentials for the next run
    with open(path, 'w') as token:
        token.write(creds.to_json())
    return creds

def insert_into_sheets():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file",
             "https://www.googleapis.com/auth/drive"]
    df_basic_info = get_basic_info()
    df_basic_info = pd.DataFrame(df_basic_info)
    df_detail_info = get_advanced_info()[0]
    df_detail_info = pd.DataFrame(df_detail_info)
    df = pd.merge(df_basic_info, df_detail_info, on='video_id', how='inner')
    df.to_csv('video_data.csv', index=False)
    credentials = ServiceAccountCredentials.from_json_keyfile_name('youtube-stat-367815-9208f10cec8b.json', scope)
    client = gspread.authorize(credentials)

    spreadsheet_video_info = client.open('video_info')
    spreadsheet_timebased_info = client.open('timebased_info')

    with open('video_data.csv', 'r', encoding="utf8") as file_obj:
        content = file_obj.read()
        client.import_csv(spreadsheet_video_info.id, data=content)

    df_time_based_info = pd.DataFrame(get_advanced_info()[1])
    df_time_based_info.to_csv('time_based_video_data_.csv', index=False)
    with open('time_based_video_data_.csv', 'r', encoding="utf8") as file_obj:
        content = file_obj.read()
        client.import_csv(spreadsheet_timebased_info.id, data=content)

def insert_into_db():
    print('Началось обновление базы...')
    df_basic_info = get_basic_info()
    df_basic_info = pd.DataFrame(df_basic_info)
    df_detail_info = get_advanced_info()
    df_detail_info = pd.DataFrame(df_detail_info)
    df = pd.merge(df_basic_info, df_detail_info, on='video_id', how='inner')
    engine = create_engine('postgresql://postgres:Qzectb1!@localhost:5432/YT')
    df.to_sql('detail_info', con=engine, if_exists='replace', index=False)
    print('Обновление базы завершено')


# df_basic_info = pd.DataFrame(get_basic_info())
# df_detail_info = get_advanced_info()[0]
# df_detail_info = pd.DataFrame(df_detail_info)
# df = pd.merge(df_basic_info, df_detail_info, on='video_id', how='inner')

df_time_based_info = pd.read_csv('time_based_video_data_.csv')
df = pd.read_csv('video_data.csv')
external_stylesheets = [dbc.themes.ZEPHYR]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


app.layout = html.Div([
    dbc.Row([
        dbc.Col(
            html.Div(html.H4('METRICS OF YOUTUBE CHANNELS'),
                     ), width={'size': 3}, style={'margin-left': '20px',
                                                  'margin-top': '10px'}
        ),
        dbc.Col(
            html.Div([
                dcc.DatePickerRange(
                    start_date=df_time_based_info['date'].min(),
                    end_date=df_time_based_info['date'].max(),
                    id='date_selector',
                    display_format='DD-MM-YYYY'),

            ]), width={'size': 3}, className='component-style'
        ),
        dbc.Col([
            html.Div([dcc.Dropdown(
                    options=[{'label': x, 'value': x} for x in df['title'].unique()],
                    value='Служебное огнестрельное оружие в работе охранника. Травмат. | дудл видео для бизнеса',
                    placeholder="Select a video",
                    id='title_selector',
                    multi=False,
                    className='component-style'
                        ),
            ])
        ]),
    dbc.Row([
        dbc.Col([
                html.Div([dcc.Graph(id='views', className='indicator-style')],
                         ),

                ], width={'size': 3}
                ),
        dbc.Col(html.Div([dcc.Graph(id='hours', className='indicator-style'),
                          ],
                         ),
                width={'size': 2}
                ),
        dbc.Col(html.Div([dcc.Graph(id='likes', className='indicator-style'),
                          ],
                         ),
                width={'size': 2}
                ),
        dbc.Col(html.Div([dcc.Graph(id='subscribers', className='indicator-style'),
                          ],
                         ),
                width={'size': 2}
                ),
        dbc.Col(html.Div([dcc.Graph(id='card_clicks', className='indicator-style'),
                          ]
                         ),
                width={'size': 3}
                ),
    ], className={}
                ),
    dbc.Row([dcc.Graph(id='plot')]),

    dbc.Row([
            dbc.Col([
                html.Div([dcc.Graph(id='funnel', config={'displayModeBar': False})],
                         ),
                    ], width={'size': 6}
                ),
            dbc.Col([
                    html.Div([
                        dbc.Row([
                            dbc.Card(
                                dbc.CardBody([
                                    html.Div("Attention", style={
                                        'background-color': '#1ca087',
                                        'text-align': 'center',
                                        'font-size': '30px',
                                    }),
                                    html.Div([dcc.Graph(id='views_difference', className='card-style')]),
                                            ])
                            ), ]),
                        dbc.Row([
                            dbc.Card(
                                dbc.CardBody([
                                    html.Div("Interest", style={
                                        'background-color': '#5cae5d',
                                        'text-align': 'center',
                                        'font-size': '30px',
                                    }),
                                    html.Div([dcc.Graph(id='hours_difference', className='card-style')]),
                                    html.Div(children=[html.H4("Total Watch Time hours"),
                                                       html.H4(id='total_watch_time')], className='card-div'),
                                    html.Div(children=[html.H4("Average Watch Time"),
                                                       html.H4(id='average_watch_time')], className='card-div'),
                                    html.Div(children=[html.H4("Likes/Views %"),
                                                       html.H4(id='likes_ratio')], className='card-div')
                                ]),
                            )]),
                        dbc.Row(
                            [dbc.Card(
                                dbc.CardBody([
                                    html.Div("Desire", style={
                                        'background-color': '#a8b223',
                                        'text-align': 'center',
                                        'font-size': '30px',
                                    }),
                                    html.Div([dcc.Graph(id='subscribers_gained', className='card-style')]),
                                    html.Div(children=[html.H4("Subscribers/Views %"),
                                                       html.H4(id='subscribers_views')], className='card-div'),
                                    html.Div(children=[html.H4("Comments/Views %"),
                                                       html.H4(id='comments_views')], className='card-div'),
                                ]

                                )
                            )

                            ]
                        ),
                        dbc.Row(
                            [dbc.Card(
                                dbc.CardBody([
                                    html.Div("Action", style={
                                        'background-color': '#ffa600',
                                        'text-align': 'center',
                                        'font-size': '30px',
                                    }),
                                    html.Div([dcc.Graph(id='clicks_difference', className='card-style')]),
                                    html.Div(children=[html.H4("Clicks/Views %"),
                                                       html.H4(id='clicks_views')], className='card-div'),
                                ])
                            )]
                        )
                        ])
            ], width={'size': 6}, style={'margin-top': '30px'}
            ),
    ]
    ),

    ])
])


@app.callback([
    Output(component_id='views', component_property='figure'),
    Output(component_id='hours', component_property='figure'),
    Output(component_id='likes', component_property='figure'),
    Output(component_id='subscribers', component_property='figure'),
    Output(component_id='card_clicks', component_property='figure'),
    Output(component_id='plot', component_property='figure'),
    Output(component_id='views_difference', component_property='figure'),
    Output(component_id='hours_difference', component_property='figure'),
    Output(component_id='total_watch_time', component_property='children'),
    Output(component_id='average_watch_time', component_property='children'),
    Output(component_id='likes_ratio', component_property='children'),
    Output(component_id='subscribers_gained', component_property='figure'),
    Output(component_id='subscribers_views', component_property='children'),
    Output(component_id='comments_views', component_property='children'),
    Output(component_id='clicks_difference', component_property='figure'),
    Output(component_id='clicks_views', component_property='children'),
            ],
    [
    Input(component_id='date_selector', component_property='start_date'),
    Input(component_id='date_selector', component_property='end_date')
    ],
              )
def filtered_dashboard(start_date, end_date):
    time_delta = (dt.datetime.strptime(end_date, "%Y-%m-%d").date()
                  - dt.datetime.strptime(start_date, "%Y-%m-%d").date()).days
    time_based_df = df_time_based_info.sort_values(by='date')
    time_based_df['views_period_ago'] = time_based_df['views'].shift(time_delta)
    time_based_df['minutes_period_ago'] = time_based_df['estimatedMinutesWatched'].shift(time_delta)
    time_based_df['subscribers_period_ago'] = time_based_df['subscribersGained'].shift(time_delta)
    time_based_df['clicks_period_ago'] = time_based_df['cardClicks'].shift(time_delta)
    time_based_df = time_based_df[(time_based_df['date'] >= start_date) & (time_based_df['date'] <= end_date)]
    fig_plot = px.line(time_based_df, x='date', y=time_based_df.columns[[2, 10]], height=300)
    fig_plot.update_layout(
        showlegend=True,
        plot_bgcolor='rgba(0,0,0,0)',
        legend=dict(
        orientation="h",
        yanchor="top",
        entrywidth=300,
        y=1,
        xanchor="left",
        x=0.02,
        ),
        legend_font={'size': 19, 'family': 'Optima, sans-serif'},
        legend_title=None,

    )
    fig_plot.update_xaxes(visible=True, title=None, tickfont_size=22, tickfont_family='Optima, sans-serif')
    fig_plot.update_yaxes(visible=True, title=None, tickfont_size=22, tickfont_family='Optima, sans-serif')
    views = go.Figure(go.Indicator(value=time_based_df['views'].sum(),
                                   title={'text': "Views",
                                          'font': {'size': 20}},
                                   number={'font': {'size': 50}},
                                   )
                      )
    hours = go.Figure(go.Indicator(value=round(time_based_df['estimatedMinutesWatched'].sum() / 60, 0),
                                   title={'text': "Total Watch Time hours",
                                          'font': {'size': 20}},
                                   number={'font': {'size': 50}}
                                   )
                      )
    likes = go.Figure(go.Indicator(value=time_based_df['likes'].sum(),
                                   title={'text': "Video Likes Added",
                                          'font': {'size': 20}},
                                   number={'font': {'size': 50}}
                                   )
                      )
    subscribers = go.Figure(go.Indicator(value=time_based_df['subscribersGained'].sum(),
                                         title={'text': "User Subscriptions Added",
                                                'font': {'size': 20}},
                                         number={'font': {'size': 50}}
                                   )
                      )
    card_clicks = go.Figure(go.Indicator(value=time_based_df['cardClicks'].sum(),
                                         title={'text': "Info Card Clicks",
                                                'font': {'size': 20}},
                                         number={'font': {'size': 50}}
                                   )
                      )

    views_difference = go.Figure(go.Indicator(value=(time_based_df['views'].sum() - time_based_df['views_period_ago'].sum()),
                                              title={'text': "Views PoP", 'font': {'size': 20}},
                                              number={'font': {'size': 50}},
                                              mode="number+delta",
                                              delta={'reference': time_based_df['views'].sum(),
                                                     'relative': True,
                                                     'position': "bottom",
                                                     "valueformat": ".0%"}
                                   )
                      )

    hours_difference = go.Figure(go.Indicator(value=(
            round((time_based_df['estimatedMinutesWatched'].sum() - time_based_df['minutes_period_ago'].sum()) / 60, 1)),
                                              title={'text': "Hours watched PoP",
                                                     'font': {'size': 20}},
                                              number={'font': {'size': 50}},
                                              mode="number+delta",
                                              delta={'reference': time_based_df['estimatedMinutesWatched'].sum() / 60,
                                                     'relative': True,
                                                     'position': "bottom", "valueformat": ".0%"}
                                   )
                      )

    total_watch_time = round(time_based_df['estimatedMinutesWatched'].sum() / 60, 0)
    average_watch_time = round(
        (time_based_df['estimatedMinutesWatched'].sum() / time_based_df['estimatedMinutesWatched'].count()), 0)
    average_watch_time = str(dt.timedelta(seconds=average_watch_time))
    likes_ratio = round(time_based_df['likes'].sum() / time_based_df['views'].sum() * 100, 2)

    subscribers_difference = go.Figure(go.Indicator(value=(
            time_based_df['subscribersGained'].sum() - time_based_df['subscribers_period_ago'].sum()),
                                              title={'text': "Subscribers Gained PoP",
                                                     'font': {'size': 20}},
                                              number={'font': {'size': 50}},
                                              mode="number+delta",
                                              delta={'reference': time_based_df['subscribersGained'].sum(),
                                                     'relative': True,
                                                     'position': "bottom",
                                                     "valueformat": ".0%"}
                                   )
                      )
    subscribers_views = round(time_based_df['subscribersGained'].sum() / time_based_df['views'].sum() * 100, 2)
    subscribers_comments = round(time_based_df['comments'].sum() / time_based_df['views'].sum() * 100, 2)

    clicks_difference = go.Figure(go.Indicator(value=(
            time_based_df['cardClicks'].sum() - time_based_df['clicks_period_ago'].sum()),
                                              title={'text': "Clicks PoP",
                                                     'font': {'size': 20}},
                                              number={'font': {'size': 50}},
                                              mode="number+delta",
                                              delta={'reference': time_based_df['cardClicks'].sum(),
                                                     'relative': True,
                                                     'position': "bottom",
                                                     "valueformat": ".0%"}
                                   )
                      )
    clicks_views = round(time_based_df['cardClicks'].sum() / time_based_df['views'].sum() * 100, 2)

    return views, hours, likes, subscribers, card_clicks, fig_plot, views_difference, hours_difference, \
        total_watch_time, average_watch_time, likes_ratio, subscribers_difference, subscribers_views, \
        subscribers_comments, clicks_difference, clicks_views


@app.callback(
    Output(component_id='funnel', component_property='figure'),
    [
    Input(component_id='title_selector', component_property='value'),
    ],
              )
def filtered_dashboard(value):
    filtered_df = df[df['title'] == value]

    funnel = go.Figure(go.Funnel(y=["Views", "Hours watched", "Subscribers", "CardClicks"],
                                 x=[filtered_df['views'].sum(),
                                   round(filtered_df['estimatedMinutesWatched'].sum() / 60, 0),
                                   filtered_df['subscribersGained'].sum(),
                                   filtered_df['cardClicks'].sum()
                                   ],
                                 textinfo="value+percent initial",
                                 textposition="outside",
                                 opacity=0.65,
                                 marker={"color": ["#1ca087", "#5cae5d", "#a8b223", "#ffa600"]},
                                 connector={"line": {"color": "royalblue", "width": 1}},
                              )
                       )
    funnel.update_layout(
        autosize=True,
        height=1200,
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=0, r=0, b=0, l=0),
        )
    funnel.update_yaxes(visible=False, title=None)
    funnel.update_traces(textfont_size=22, textfont_family='Optima, sans-serif')

    return funnel


if __name__ == '__main__':
    # insert_into_sheets()
    app.run_server(host='127.0.0.1', port=18273, debug=True)
