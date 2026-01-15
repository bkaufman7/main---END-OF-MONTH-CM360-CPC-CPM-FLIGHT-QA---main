const CT_RULES = [
  { tag: 'explicit_click_tracker', w: 3, re: /\b(click\s*tracker|click\s*tag|clktrk|clk\s*trk|ctrk|rdir|redirect(?:or)?)\b/i },
  { tag: 'pixel_1x1',              w: 3, re: /(^|[^\d])1\s*[x×]\s*1([^\d]|$)|\b1x1cc\b|\b1x1\b/i },
  { tag: 'affiliate_cj',           w: 2, re: /\b(commission\s*junction|cj(?:\b|_|$)|affiliate)\b/i },
  { tag: 'emailish',               w: 2, re: /\b(sfmc|email|recommendation\s*emails|away\s*message)\b/i },
  { tag: 'social',                 w: 1, re: /\b(facebook|fbk|meta|instagram|ig|tiktok|ttk|tt|snapchat|sc|pinterest|pin)\b/i },
  { tag: 'adtech_vendor',          w: 1, re: /\b(dv360|rokt|sizmek|nyt\.com|imdb|morning\s*brew|amazon\s+advertising|mobile\s+fuse|pandora|sxm|yahoo|programmatic|trueview|ott)\b/i },
  { tag: 'creative_pixel_combo',   w: 1, re: /\b(jpg|vid|video)\b.*1\s*[x×]\s*1/i },
];