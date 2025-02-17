# WoWAudit Bot and Quartz Team Charts

Welcome to the utter mess that is this repo!

This contains a bot to pull down the raw JSON data that goes into a WoWAudit sheet once per hour and tosses it into
a MongoDB timeseries collection. It does some minor cleanup on column names to account for the hacks WoWAudit uses
there.

This also contains the notebooks for generating the charts used at https://maggiesuewow.github.io/QuartzTWWS1/.
These are charts based on the wowaudit data which show fun things over the course of the season, rather than just
what we can see currently on the audit sheet.

*TODO: split the charting code into another repo.*

# Huge Disclaimer

I'm not a data scientist, and I've only dabbled in Mongo and Pandas. I'm using Plotly for the charts, but I know very
little about it. I relied heavily on ChatGPT for all of the data processing and charting. **Heavily**.
Like, seriously, I haven't made any effort to learn Plotly, and I'm a Pandas n00b.

So the charting code here isn't pretty. At all.

But it works, and I've put the amount of time I care to into it.

If you're keen to help with this stuff feel free to hit me up in-game.

# Requirements

* Python 3.11.9+
* Pandas, Plotly, etc. See requirements.txt, and see what's missing when you run it.
* MongoDB
* Docker

The bot needs a config parameter, `wowaudit_sheet_key`, which is the wowaudit key you find on the settings page of the
sheet.

Example config.yaml:

```yaml
mongodb_uri: "mongodb://hostname:27017/"
wowaudit_sheet_key: 'xxxx'
```

*TODO: Add info on the Mongo collection.*

# License

Code is licensed under the MIT license except as otherwise noted.
See [LICENSE](https://github.com/MaggieSueWoW/WoW/WoWAudit/blob/master/LICENSE) for details.
