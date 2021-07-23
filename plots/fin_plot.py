import requests, os
import plotly.graph_objects as go

def save_sec_plot(secid):

    stock_host = os.environ.get('STOCK_HOST')

    url = stock_host + '/' + secid

    with requests.get(url) as response:

        data = [(r['bos'], r['date'].split('T')[0], r['close'])for r in response.json()]

        data.sort(key=lambda x: x[1])

    layout = {
        'plot_bgcolor': '#ffffff',
        "xaxis": {
            'fixedrange': True,
            'tickfont': {
                'color': '#6c757d'
            },
            'gridcolor': '#f2f3f2'
        },
        "yaxis": {
            'fixedrange': True,
            'tickfont': {
                'color': '#6c757d'
            },
            'gridcolor': '#f2f3f2',
            'range': [min(data, key=lambda x: x[2])[2], max(data, key=lambda x: x[2])[2]]
        },
        "margin": {
            "l": 0,
            "r": 0,
            "b": 0,
            "t": 0
        },
        "showlegend": False
    }

    fig = go.Figure(layout=layout)

    for i in range(len(data) - 1):

        linecolor = 'rgba(17, 203, 91, 1.0)' if data[i][0] >= 0 else 'rgba(245, 61, 87, 1.0)'
        fillcolor = 'rgba(17, 203, 91, 0.05)' if data[-1][0] >= 0 else 'rgba(245, 61, 87, 0.05)'

        fig.add_trace(go.Scatter(x=[data[i][1], data[i+1][1]], y=[data[i][2], data[i+1][2]],
                                 mode='lines', line={'color': linecolor},
                                 fill='tozeroy', fillcolor=fillcolor)
                      )

    fig.update_layout(showlegend=False)

    fig.update_traces(hoverinfo="x,text")
    stock_dir = os.environ.get('STOCK_DIR')
    fig.write_html(stock_dir+secid+".html", config=dict(displayModeBar=False), full_html=False)


def upd_secs_plots():

    url = os.environ.get('STOCK_HOST')

    with requests.get(url) as response:

        for sec in response.json():

            save_sec_plot(sec['sec_id'])

if __name__ == '__main__':
    upd_secs_plots()