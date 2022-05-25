from src.features.general_features import *


def process_http(http_df):
    http_df['url_length'] = http_df['url'].apply(lambda x: len(x))
    http_df['url_depth'] = http_df['url'].apply(lambda x: len(x.split('/')) - 1)
    http_df['http_content_length'] = http_df['content'].apply(lambda x: len(x.split()))
    return http_df


def parse_http(x):
    """Adapted from: https://github.com/lcd-dal/feature-extraction-for-CERT-insider-threat-test-dataset/blob/7e1572418d38f805c25846d940aeb7af4eb99e74/feature_extraction.py#L307"""
    if x.sld in ['dropbox.com', 'drive.google.com', 'mega.co.nz', 'account.live.com']:
        return "cloud_webvisit"

    elif x.sld in ['wikileaks.org', 'freedom.press', 'theintercept.com']:
        return "hacktivist_webvisit"

    elif x.sld in ['facebook.com', 'twitter.com', 'plus.google.com', 'instagr.am', 'instagram.com',
                   'flickr.com', 'linkedin.com', 'reddit.com', 'about.com', 'youtube.com', 'pinterest.com',
                   'tumblr.com', 'quora.com', 'vine.co', 'match.com', 't.co']:
        return "social_webvisit"

    elif x.sld in ['indeed.com', 'monster.com', 'careerbuilder.com', 'simplyhired.com']:
        return "jobsearch_webvisit"

    elif ('job' in x.url and ('hunt' in x.url or 'search' in x.url)):
        return "jobsearch_webvisit"
    else:
        return "webvisit"


def http_statistical_features(http_df, return_stats=True):
    http_df['date'] = pd.to_datetime(http_df['date'])
    http_df['day'] = pd.to_datetime(http_df['date']).dt.normalize()
    http_df['activity'] = http_df.apply(parse_http, axis=1)
    http_df = process_http(http_df)

    http_daily_stats = http_df.groupby(["day", "user"])['url_length', 'url_depth', 'http_content_length'] \
        .mean().reset_index()

    if return_stats:
        return http_daily_stats
    else:
        return http_df


def http_count_features(http_df):
    http_df = http_statistical_features(http_df, False)
    groupby_cols = ['day', 'user', 'activity']

    http_daily_counts = pd.DataFrame({"count": http_df.groupby(groupby_cols).size()}).reset_index()
    http_daily_counts = http_daily_counts.set_index(groupby_cols).unstack(['activity'])

    http_daily_counts.columns = ['{}_{}'.format(a, b) for a, b in http_daily_counts.columns]

    http_daily_counts.fillna(0, inplace=True)
    http_daily_counts = http_daily_counts.reset_index()

    return http_daily_counts


def http_merged_features(http_df):
    http_daily_stats = http_statistical_features(http_df)
    http_daily_counts = http_count_features(http_df)

    http_daily_file = http_daily_counts.merge(http_daily_stats, on=['day', 'user'])

    return http_daily_file
