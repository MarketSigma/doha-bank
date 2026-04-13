"""
Doha Bank Market Intelligence — v6
Exact match to reference image:
- Page 2: full-width 2-col news grid, cards SHORT and WIDE
- Global news: 4 cards in 2x2 grid (full page width)
- Qatar news: 4 cards in 2x2 grid below (full page width)
- Badge: small right-aligned box, ~18mm wide
- Card height: ~28mm (short and wide like reference)
"""
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas as pdfcanvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import os, sys
from datetime import date as dt

pdfmetrics.registerFont(TTFont('Caladea',        '/usr/share/fonts/truetype/crosextra/Caladea-Regular.ttf'))
pdfmetrics.registerFont(TTFont('Caladea-Bold',   '/usr/share/fonts/truetype/crosextra/Caladea-Bold.ttf'))
pdfmetrics.registerFont(TTFont('Caladea-Italic', '/usr/share/fonts/truetype/crosextra/Caladea-Italic.ttf'))
pdfmetrics.registerFont(TTFont('Carlito',        '/usr/share/fonts/truetype/crosextra/Carlito-Regular.ttf'))
pdfmetrics.registerFont(TTFont('Carlito-Bold',   '/usr/share/fonts/truetype/crosextra/Carlito-Bold.ttf'))
pdfmetrics.registerFont(TTFont('Carlito-Italic', '/usr/share/fonts/truetype/crosextra/Carlito-Italic.ttf'))

W, H = landscape(A4)
M    = 11*mm
UW   = W - 2*M

# ── DOHA BANK COLOURS ─────────────────────────────────────────────────────
BLUE    = colors.HexColor('#1a5fa8')
NAVY    = colors.HexColor('#0d2c5e')
CYAN    = colors.HexColor('#00aeef')
GOLD    = colors.HexColor('#c9a84c')
WHITE   = colors.white
OFFWHT  = colors.HexColor('#f4f8fd')
TBLHDR  = colors.HexColor('#0f3d7a')
RULE    = colors.HexColor('#c5d8ee')
RULE_DK = colors.HexColor('#7aafd4')
TEXT    = colors.HexColor('#1a2a3a')
MUTED   = colors.HexColor('#5a7a96')
UP      = colors.HexColor('#1a7a45')
DOWN    = colors.HexColor('#c0392b')
SUBT    = colors.HexColor('#9ac4e8')

HDR_H = 24*mm
FTR_H =  5.5*mm
KPI_H = 14*mm
SEC_H =  5.5*mm
ROW_H =  4.6*mm
GAP   =  2.5*mm

def pct_col(v):
    v=str(v)
    if v.startswith('+'): return UP
    if v.startswith('-'): return DOWN
    return MUTED

def fr(c,x,y,w,h,col):
    c.setFillColor(col); c.rect(x,y,w,h,fill=1,stroke=0)

def sr(c,x,y,w,h,col,lw=0.4):
    c.setStrokeColor(col); c.setLineWidth(lw)
    c.rect(x,y,w,h,fill=0,stroke=1)

def t(c,txt,x,y,font='Carlito',size=8,color=TEXT,align='left',maxw=None):
    c.setFont(font,size); c.setFillColor(color)
    if maxw:
        while len(txt)>4 and c.stringWidth(txt,font,size)>maxw:
            txt=txt[:-4]+'...'
    if   align=='right':  c.drawRightString(x,y,txt)
    elif align=='center': c.drawCentredString(x,y,txt)
    else:                 c.drawString(x,y,txt)

def hl(c,x1,y,x2,col=RULE,lw=0.35):
    c.setStrokeColor(col); c.setLineWidth(lw); c.line(x1,y,x2,y)

def ml(c,txt,x,y,font,size,color,maxw,lh,maxl=3):
    c.setFont(font,size); c.setFillColor(color)
    words=txt.split(); lines=[]; line=''
    for w in words:
        tt=(line+' '+w).strip()
        if c.stringWidth(tt,font,size)<=maxw: line=tt
        else:
            if line: lines.append(line)
            line=w
            if len(lines)>=maxl: break
    if line and len(lines)<maxl: lines.append(line)
    for i,ln in enumerate(lines[:maxl]):
        c.drawString(x,y-i*lh,ln)
    return y-len(lines[:maxl])*lh

# ── HEADER ────────────────────────────────────────────────────────────────
def draw_header(c,report_date,page=1,total=2):
    fr(c,0,H-HDR_H,W,HDR_H,BLUE)
    fr(c,0,H-HDR_H,56*mm,HDR_H,NAVY)
    c.setStrokeColor(colors.HexColor('#2a6fc0')); c.setLineWidth(0.5)
    c.line(56*mm,H-HDR_H+3*mm,56*mm,H-3*mm)
    c.setFillColor(WHITE); c.setStrokeColor(CYAN); c.setLineWidth(0.8)
    c.roundRect(5*mm,H-HDR_H+5*mm,12*mm,12*mm,1.5*mm,fill=1,stroke=1)
    t(c,'D',11*mm,H-HDR_H+10*mm,'Caladea-Bold',10,BLUE,'center')
    t(c,'بنك الدوحة',20*mm,H-HDR_H+18*mm,'Carlito',7,SUBT)
    t(c,'DOHA BANK',20*mm,H-HDR_H+11.5*mm,'Caladea-Bold',10,WHITE)
    c.setStrokeColor(GOLD); c.setLineWidth(0.8)
    c.line(W/2-52*mm,H-HDR_H+14*mm,W/2-26*mm,H-HDR_H+14*mm)
    c.line(W/2+26*mm,H-HDR_H+14*mm,W/2+52*mm,H-HDR_H+14*mm)
    t(c,'MARKET INTELLIGENCE',W/2,H-HDR_H+19*mm,'Caladea-Bold',14,WHITE,'center')
    t(c,report_date,W/2,H-HDR_H+12.5*mm,'Carlito',9,GOLD,'center')
    t(c,'Market Snapshot  |  Currency & Fixed Income  |  Global & Qatar News',
      W/2,H-HDR_H+7*mm,'Carlito-Italic',6.5,SUBT,'center')
    t(c,f'Page {page} of {total}',W-M,H-HDR_H+19*mm,'Carlito',6.5,GOLD,'right')
    t(c,'Generated  07:00 AST',W-M,H-HDR_H+14*mm,'Carlito',6,SUBT,'right')
    t(c,'Yahoo Finance  ·  Reuters  ·  Bloomberg',W-M,H-HDR_H+9.5*mm,'Carlito',5.5,SUBT,'right')
    t(c,'The Peninsula  ·  Qatar Tribune',W-M,H-HDR_H+5.5*mm,'Carlito',5.5,SUBT,'right')
    fr(c,0,H-HDR_H,W,1.5*mm,CYAN)
    return H-HDR_H

def draw_footer(c,report_date):
    fr(c,0,0,W,FTR_H,BLUE)
    fr(c,0,FTR_H-0.7*mm,W,0.7*mm,CYAN)
    t(c,'Sources: Yahoo Finance  ·  Reuters  ·  Bloomberg  ·  The Peninsula  ·  Qatar Tribune    |    Strictly Confidential — Doha Bank HNWI Clients Only. Not for redistribution.',
      M,2*mm,'Carlito-Italic',5,SUBT)
    t(c,f'Doha Bank Market Intelligence  ·  {report_date}',W-M,2*mm,'Carlito',5.5,WHITE,'right')

def draw_kpi(c,y,kpis):
    cw=UW/len(kpis)
    barcols=[MUTED,UP,GOLD,BLUE,DOWN,CYAN]
    for i,(val,vtype,lbl,sub) in enumerate(kpis):
        cx=M+i*cw
        fr(c,cx,y-KPI_H,cw,KPI_H,WHITE)
        sr(c,cx,y-KPI_H,cw,KPI_H,RULE_DK,0.5)
        fr(c,cx,y-2*mm,cw,2*mm,NAVY)
        fr(c,cx,y-KPI_H,cw,1.2*mm,barcols[i%len(barcols)])
        vcol=UP if vtype=='up' else(DOWN if vtype=='dn' else NAVY)
        t(c,val,cx+3*mm,y-6.5*mm,'Caladea-Bold',11,vcol)
        t(c,lbl,cx+3*mm,y-9.5*mm,'Carlito-Bold',6.5,TEXT)
        t(c,sub,cx+3*mm,y-13*mm,'Carlito',5.5,MUTED)
    return y-KPI_H-2*mm

def sec_hdr(c,x,y,title,w):
    fr(c,x,y-SEC_H,w,SEC_H,BLUE)
    fr(c,x,y-SEC_H,2*mm,SEC_H,CYAN)
    t(c,f'| {title}',x+3*mm,y-3.6*mm,'Caladea-Bold',7,WHITE)
    return y-SEC_H

def draw_table(c,x,y,hdrs,rows,tw,cws):
    fr(c,x,y-ROW_H,tw,ROW_H,TBLHDR)
    hl(c,x,y-ROW_H,x+tw,CYAN,0.5)
    cx=x
    for i,(h,cw) in enumerate(zip(hdrs,cws)):
        if i==0: t(c,h,cx+2*mm,y-ROW_H+1.5*mm,'Carlito-Bold',6,WHITE)
        else:    t(c,h,cx+cw/2,y-ROW_H+1.5*mm,'Carlito-Bold',6,WHITE,'center')
        cx+=cw
    y-=ROW_H
    for ri,row in enumerate(rows):
        bg=OFFWHT if ri%2==0 else WHITE
        fr(c,x,y-ROW_H,tw,ROW_H,bg)
        hl(c,x,y-ROW_H,x+tw,RULE,0.2)
        cx=x
        for ci,(cell,cw) in enumerate(zip(row,cws)):
            cell=str(cell)
            if ci==0:
                t(c,cell,cx+2*mm,y-ROW_H+1.5*mm,'Carlito-Bold',7,TEXT,'left',cw-3*mm)
            else:
                col=pct_col(cell)
                fw='Carlito-Bold' if('%' in cell or 'bps' in cell or cell in('—','Pegged'))else'Carlito'
                t(c,cell,cx+cw-1.5*mm,y-ROW_H+1.5*mm,fw,7,col,'right')
            cx+=cw
        y-=ROW_H
    hl(c,x,y,x+tw,RULE_DK,0.4)
    return y-1.5*mm

def cw5(w): return[w*0.37,w*0.18,w*0.15,w*0.15,w*0.15]

# ── NEWS CARD — exact match to reference ─────────────────────────────────
# Reference card: SHORT and WIDE, headline bold top-left, summary small below,
# badge is a SMALL box on the RIGHT (~18mm wide, full card height)
def draw_news_card(c, x, y, w, h, item):
    metric = str(item.get('metric',       '—'))
    mlbl   = str(item.get('metric_label', ''))
    src    = str(item.get('source',       ''))
    hl_txt = str(item.get('headline',     ''))
    summ   = str(item.get('summary',      ''))

    # In reference: badge is ~18-20mm wide on the RIGHT
    BW = 18*mm

    # Card: white background, thin border all around
    fr(c, x, y-h, w, h, WHITE)
    sr(c, x, y-h, w, h, RULE, 0.5)

    # ── BADGE: small box RIGHT side, navy bg ─────────────────────────────
    bx = x + w - BW
    fr(c, bx, y-h, BW, h, NAVY)
    # cyan left edge of badge
    fr(c, bx, y-h, 0.8*mm, h, CYAN)

    # metric value centred in badge
    msz = 9 if len(metric) <= 7 else (7.5 if len(metric) <= 10 else 6.5)
    t(c, metric, bx+BW/2+0.4*mm, y-h/2+1.5*mm, 'Caladea-Bold', msz, CYAN, 'center')

    # label below metric
    mw = BW - 3*mm
    if c.stringWidth(mlbl, 'Carlito', 5) <= mw:
        t(c, mlbl, bx+BW/2+0.4*mm, y-h+2.5*mm, 'Carlito', 5, SUBT, 'center')
    else:
        words = mlbl.split(); ln1=''; ln2=''
        for w_ in words:
            test=(ln1+' '+w_).strip()
            if c.stringWidth(test,'Carlito',5)<=mw: ln1=test
            else: ln2=(ln2+' '+w_).strip()
        t(c, ln1, bx+BW/2+0.4*mm, y-h+5*mm,   'Carlito', 5, SUBT, 'center')
        t(c, ln2, bx+BW/2+0.4*mm, y-h+2.5*mm, 'Carlito', 5, SUBT, 'center')

    # ── TEXT BODY: left side ──────────────────────────────────────────────
    tx  = x + 3*mm
    tw2 = w - BW - 5*mm

    # headline bold — starts from top of card
    hl_bot = ml(c, hl_txt, tx, y-3*mm, 'Caladea-Bold', 7.5, TEXT, tw2, 2.7*mm, 2)

    # source small cyan
    t(c, src.upper(), tx, hl_bot-2*mm, 'Carlito-Bold', 5.5, CYAN)

    # thin rule
    hl(c, tx, hl_bot-3.2*mm, tx+tw2, RULE, 0.25)

    # summary italic small — fills remaining space
    ml(c, summ, tx, hl_bot-5.5*mm, 'Carlito-Italic', 6, MUTED, tw2, 2.3*mm, 4)

# ── NEWS SECTION: full-width 2-col grid ───────────────────────────────────
def draw_news_section(c, x, y, title, items, total_w, card_h, card_gap=2*mm):
    """
    Draws section header full width, then 4 items in a 2×2 grid.
    Returns y after all cards drawn.
    """
    y = sec_hdr(c, x, y, title, total_w)
    y -= 1.5*mm

    card_w = (total_w - card_gap) / 2

    for i, item in enumerate(items[:4]):
        row = i // 2
        col = i % 2
        cx = x + col * (card_w + card_gap)
        cy = y - row * (card_h + card_gap)
        draw_news_card(c, cx, cy, card_w, card_h, item)

    rows_used = min(2, (len(items[:4]) + 1) // 2)
    return y - rows_used * (card_h + card_gap)

# ── PAGE 1 ────────────────────────────────────────────────────────────────
def page1(c, report_date, data):
    fr(c,0,0,W,H,WHITE)
    top=draw_header(c,report_date,1,2)
    y=top-1.5*mm

    kpi_rows=[]
    for k in data.get('kpis',[]):
        val=str(k.get('value','—'))
        vt='up' if '+' in val else('dn' if val.startswith('-') else 'neu')
        kpi_rows.append((val,vt,k.get('label',''),k.get('sublabel','')))
    y=draw_kpi(c,y,kpi_rows)

    def rows(sec):
        out=[]
        for r in data.get(sec,[]):
            px=r.get('px_last','N/A')
            px=f"{px:,.2f}" if isinstance(px,float) else str(px)
            out.append([r.get('name',''),px,
                        r.get('change_1d','—'),r.get('mtd','—'),r.get('ytd','—')])
        return out

    cw2=(UW-4*mm)/2
    xL=M; xR=M+cw2+4*mm

    yL=y
    yL=sec_hdr(c,xL,yL,'GLOBAL INDICES',cw2)
    yL=draw_table(c,xL,yL,['Market / Index','PX Last','1D %','MTD %','YTD %'],rows('global_indices'),cw2,cw5(cw2))
    yL-=GAP
    yL=sec_hdr(c,xL,yL,'SPOT CURRENCY',cw2)
    yL=draw_table(c,xL,yL,['Pair','PX Last','1D %','MTD %','YTD %'],rows('spot_currency'),cw2,cw5(cw2))
    yL-=GAP
    yL=sec_hdr(c,xL,yL,'QAR CROSS RATES',cw2)
    yL=draw_table(c,xL,yL,['Pair','PX Last','1D %','MTD %','YTD %'],rows('qar_cross_rates'),cw2,cw5(cw2))
    yL-=GAP
    yL=sec_hdr(c,xL,yL,'FIXED INCOME — UST YIELDS',cw2)
    yL=draw_table(c,xL,yL,['Instrument','Yield','1D Chg','MTD Chg','YTD Chg'],rows('fixed_income'),cw2,cw5(cw2))

    yR=y
    yR=sec_hdr(c,xR,yR,'GCC & REGIONAL INDICES',cw2)
    yR=draw_table(c,xR,yR,['Market / Index','PX Last','1D %','MTD %','YTD %'],rows('gcc_indices'),cw2,cw5(cw2))
    yR-=GAP
    yR=sec_hdr(c,xR,yR,'QATARI BANKS',cw2)
    yR=draw_table(c,xR,yR,['Bank','PX Last','1D %','MTD %','YTD %'],rows('qatari_banks'),cw2,cw5(cw2))
    yR-=GAP
    yR=sec_hdr(c,xR,yR,'COMMODITIES & ENERGY',cw2)
    yR=draw_table(c,xR,yR,['Asset','PX Last','1D %','MTD %','YTD %'],rows('commodities'),cw2,cw5(cw2))

    draw_footer(c,report_date)

# ── PAGE 2 ────────────────────────────────────────────────────────────────
def page2(c, report_date, global_news, qatar_news):
    fr(c,0,0,W,H,WHITE)
    top=draw_header(c,report_date,2,2)
    y=top-2*mm

    # Calculate card height to fit both sections perfectly
    # Available: y down to FTR_H
    # Need: 2 section headers + 4 global cards (2 rows) + gap + 4 qatar cards (2 rows)
    avail = y - FTR_H - 3*mm
    # 2 section headers + 3mm gap between sections
    header_space = 2*SEC_H + 1.5*mm + 3*mm
    card_gap     = 2*mm
    # 4 cards = 2 rows, each with gap between
    # total rows space = avail - header_space
    row_space    = avail - header_space
    # 4 rows total (2 for global + 2 for qatar), 3 gaps between rows within each section
    card_h       = (row_space - 3*card_gap) / 4

    y = draw_news_section(c, M, y, 'REGIONAL & GLOBAL NEWS',
                          global_news, UW, card_h, card_gap)
    y -= 3*mm
    y = draw_news_section(c, M, y, 'QATAR NEWS',
                          qatar_news, UW, card_h, card_gap)

    draw_footer(c,report_date)

# ── GENERATE ──────────────────────────────────────────────────────────────
def generate(data, output_path):
    report_date=data.get('config',{}).get('report_date',dt.today().strftime('%d %B %Y'))
    c=pdfcanvas.Canvas(output_path,pagesize=landscape(A4))
    c.setTitle(f'Doha Bank Market Intelligence — {report_date}')
    c.setAuthor('Doha Bank — Automated Market Intelligence System')
    page1(c,report_date,data); c.showPage()
    page2(c,report_date,data.get('global_news',[]),data.get('qatar_news',[])); c.showPage()
    c.save()
    sz=os.path.getsize(output_path)
    print(f'PDF: {output_path}  |  {sz/1024:.0f} KB')
    return output_path

if __name__=='__main__':
    SAMPLE={
        'config':{'report_date':'2nd April 2026'},
        'kpis':[
            {'value':'Mixed',  'label':'Global Equities','sublabel':'US -3.9% YTD · UK +4.4% YTD'},
            {'value':'$107.74','label':'Brent Crude',    'sublabel':'+78.8% Year-to-Date'},
            {'value':'4,691',  'label':'Gold (QAR)',     'sublabel':'+6.5% YTD · Safe-haven demand'},
            {'value':'10,270', 'label':'QSE Index',      'sublabel':'+0.8% today · -4.6% YTD'},
            {'value':'4.37%',  'label':'UST 10Y Yield',  'sublabel':'+5.0% YTD · Rising yields'},
            {'value':'4.50%',  'label':'QCB Sukuk Yield','sublabel':'QR3bn · 2.7x oversubscribed'},
        ],
        'global_indices':[
            {'name':'US S&P 500',   'px_last':6575.32, 'change_1d':'+0.7%','mtd':'+0.7%','ytd':'-3.9%'},
            {'name':'UK FTSE 100',  'px_last':10364.79,'change_1d':'+1.9%','mtd':'+1.9%','ytd':'+4.4%'},
            {'name':'Japan Nikkei', 'px_last':52465.12,'change_1d':'-2.4%','mtd':'+2.7%','ytd':'+4.2%'},
            {'name':'Germany DAX',  'px_last':23298.89,'change_1d':'+2.7%','mtd':'+2.7%','ytd':'-4.9%'},
            {'name':'Hong Kong HSI','px_last':25817.23,'change_1d':'-1.1%','mtd':'+0.9%','ytd':'-2.4%'},
            {'name':'India Sensex', 'px_last':22228.45,'change_1d':'-2.0%','mtd':'-0.5%','ytd':'-14.9%'},
        ],
        'gcc_indices':[
            {'name':'Qatar QE',     'px_last':10270.69,'change_1d':'+0.8%','mtd':'+0.8%','ytd':'-4.6%'},
            {'name':'Saudi Tadawul','px_last':11275.90,'change_1d':'+0.7%','mtd':'+0.2%','ytd':'+7.5%'},
            {'name':'Dubai DFM',    'px_last':5544.61, 'change_1d':'+2.0%','mtd':'+2.0%','ytd':'-8.3%'},
            {'name':'Abu Dhabi ADX','px_last':9649.72, 'change_1d':'+1.4%','mtd':'+1.4%','ytd':'-3.4%'},
            {'name':'Kuwait Boursa','px_last':9084.53, 'change_1d':'+0.8%','mtd':'+0.8%','ytd':'-4.4%'},
            {'name':'Oman',         'px_last':8190.29, 'change_1d':'+0.3%','mtd':'+0.3%','ytd':'+39.6%'},
            {'name':'Bahrain',      'px_last':8190.29, 'change_1d':'+0.3%','mtd':'+0.3%','ytd':'+39.6%'},
        ],
        'spot_currency':[
            {'name':'USD Index','px_last':100.039,'change_1d':'+0.4%','mtd':'+0.1%','ytd':'+1.7%'},
            {'name':'EUR / USD','px_last':1.1535, 'change_1d':'-0.5%','mtd':'-0.2%','ytd':'-1.8%'},
            {'name':'GBP / USD','px_last':1.3234, 'change_1d':'-0.5%','mtd':'+0.1%','ytd':'-1.8%'},
            {'name':'CHF / USD','px_last':0.7987, 'change_1d':'-0.6%','mtd':'+0.1%','ytd':'-0.8%'},
            {'name':'USD / JPY','px_last':159.390,'change_1d':'-0.4%','mtd':'-0.4%','ytd':'-1.7%'},
            {'name':'CNY / USD','px_last':6.8871, 'change_1d':'-0.2%','mtd':'+0.1%','ytd':'+1.5%'},
        ],
        'qar_cross_rates':[
            {'name':'USD / QAR','px_last':3.6415,'change_1d':'—',    'mtd':'—',    'ytd':'Pegged'},
            {'name':'EUR / QAR','px_last':4.2055,'change_1d':'+0.5%','mtd':'+0.2%','ytd':'+1.8%'},
            {'name':'GBP / QAR','px_last':4.8250,'change_1d':'+0.6%','mtd':'—',    'ytd':'+1.8%'},
            {'name':'CHF / QAR','px_last':4.5651,'change_1d':'+0.7%','mtd':'-0.4%','ytd':'+0.8%'},
            {'name':'CNY / QAR','px_last':0.5294,'change_1d':'+0.2%','mtd':'-0.2%','ytd':'-1.5%'},
        ],
        'qatari_banks':[
            {'name':'Doha',    'px_last':3.20, 'change_1d':'-1.2%','mtd':'-1.2%','ytd':'+11.5%'},
            {'name':'QNB',     'px_last':17.10,'change_1d':'-0.4%','mtd':'+0.4%','ytd':'-8.4%'},
            {'name':'QIB',     'px_last':22.49,'change_1d':'-0.7%','mtd':'-0.7%','ytd':'-6.1%'},
            {'name':'CBQ',     'px_last':4.35, 'change_1d':'+2.0%','mtd':'+2.0%','ytd':'+3.5%'},
            {'name':'QIIB',    'px_last':11.02,'change_1d':'-0.9%','mtd':'-0.9%','ytd':'-3.6%'},
            {'name':'Al Rayan','px_last':2.19, 'change_1d':'-0.3%','mtd':'+0.3%','ytd':'-0.2%'},
            {'name':'Dukhan',  'px_last':3.45, 'change_1d':'-0.3%','mtd':'-0.3%','ytd':'-3.3%'},
            {'name':'Ahli',    'px_last':3.75, 'change_1d':'-1.3%','mtd':'-1.3%','ytd':'+0.01%'},
            {'name':'Lesha',   'px_last':1.84, 'change_1d':'+1.9%','mtd':'+1.9%','ytd':'-2.1%'},
        ],
        'commodities':[
            {'name':'Brent Crude','px_last':107.74,'change_1d':'+6.5%','mtd':'+3.6%','ytd':'+78.8%'},
            {'name':'Gold (QAR)', 'px_last':4891.40,'change_1d':'-2.5%','mtd':'+0.3%','ytd':'+6.5%'},
            {'name':'Silver',     'px_last':71.74,  'change_1d':'-3.7%','mtd':'-4.2%','ytd':'+0.8%'},
            {'name':'LNG JP/KR',  'px_last':19.83,  'change_1d':'-1.5%','mtd':'-1.5%','ytd':'+114.6%'},
        ],
        'fixed_income':[
            {'name':'UST 5-Year', 'px_last':'4.01%','change_1d':'+1.5%','mtd':'+1.7%','ytd':'+7.7%'},
            {'name':'UST 10-Year','px_last':'4.37%','change_1d':'+1.3%','mtd':'+1.3%','ytd':'+5.0%'},
        ],
        'global_news':[
            {'metric':'HIGH',    'metric_label':'Global Risk Level','source':'Reuters',
             'headline':'IMF, World Bank & IEA Coordinate on Iran War Impact',
             'summary':'Three institutions will share data, coordinate policy advice and assess financing needs as the conflict disrupts supply chains, driving oil, gas, fertiliser and food prices higher globally.'},
            {'metric':'$107.74', 'metric_label':'Brent per barrel','source':'Reuters',
             'headline':'OPEC Output Falls 7.3mn bpd — Hormuz Disruption',
             'summary':'Output at 21.57mn bpd after cuts in Kuwait, Iraq, Saudi Arabia and UAE following Strait of Hormuz disruptions. Brent at $107.74, +78.8% YTD — Qatar fiscal position strengthens.'},
            {'metric':'+6%',     'metric_label':'LME Al Futures 1D','source':'Bloomberg',
             'headline':'Iran Strikes Gulf Aluminium — LME Futures +6%',
             'summary':'Iranian strikes on Gulf aluminium facilities pushed LME futures sharply higher. Markets face tighter inventories and limited capacity to absorb further supply shocks.'},
            {'metric':'WTI $105','metric_label':'WTI Crude Price','source':'Reuters',
             'headline':'Oil +5% After Trump Confirms Continued Iran Strikes',
             'summary':'Brent rose to $107.49 and WTI to $105.40 after President Trump confirmed US strikes continue — prolonged Gulf supply disruption concerns push oil higher.'},
        ],
        'qatar_news':[
            {'metric':'+12.6%',  'metric_label':'Q3 Rental Growth','source':'The Peninsula',
             'headline':'Rental Market Strong Growth in 2025',
             'summary':'Aqarat reports rental contracts in 2025 exceeded 2024 levels across all quarters. Q3 growth +12.6%; Q4 reached 35,957 agreements — robust demand in residential and commercial segments.'},
            {'metric':'1 Apr',   'metric_label':'Service Launch Date','source':'Qatar Tribune',
             'headline':'GTA Launches Excise Tax Warehouse Licensing',
             'summary':'General Tax Authority introduces licensed premises service for excise goods under tax-suspension regime. Rollout begins 1 April 2026 for producers — key compliance milestone.'},
            {'metric':'125K',    'metric_label':'Absher Points Max','source':'The Peninsula',
             'headline':'QIB Rewards — Up to 125,000 Absher Points',
             'summary':'Campaign runs 1 April – 30 June 2026 for eligible salary-transfer customers on selected QIB cards. Rewards linked to sign-on and spending thresholds. Strong retail banking incentive.'},
            {'metric':'2.7x',    'metric_label':'Bid-to-Cover Ratio','source':'Qatar Tribune',
             'headline':'QCB Issues QR3bn Government Ijara Sukuk',
             'summary':'Issued on behalf of Ministry of Finance. Split between Jan 2029 and Aug 2030 at 4.5% yield. Total bids reached QR8bn — 2.7x oversubscribed, confirming deep market demand.'},
        ],
    }
    out=sys.argv[1] if len(sys.argv)>1 else '/mnt/user-data/outputs/DohaBank_MI_Final.pdf'
    generate(SAMPLE,out)
