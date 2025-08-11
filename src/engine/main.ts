import { arrayMode } from './../lib/array_mode';
import { countries3to2, countryCodes } from '../model/countries';
import { CountryCode, RawData, theme2category } from '../model/theme2category';
import { getTimeElapsed } from './../lib/date_time';
import { Log } from './../lib/log';
import { Site } from "../site";
import axios from 'axios';
import unzipper from 'unzipper';
import split2 from 'split2';
import { transform } from 'stream-transform';
import { Writable } from 'stream';

const SLUG = "MainEngine";
const WEIGHT = 3;
const MAX_RECORDS = 5000;
const MAX_PARSED_ITEMS = 1000;

export class MainEngine {
    static start = () => new Promise<boolean>((resolve, reject) => {
        setTimeout(() => {
            MainEngine.run();
        }, 1000);
        resolve(true);
    });

    static stop = () => new Promise<boolean>((resolve, reject) => {
        resolve(true);
    });

    private static getLatestGKGURL = () => new Promise<{
        gkg: null | string,
        exportx: null | string,
        mentions: null | string,
    }>(async (resolve, reject) => {
        let gkg: null | string = null;
        let exportx: null | string = null;
        let mentions: null | string = null;
        try {
            const r = await axios.get(`http://data.gdeltproject.org/gdeltv2/lastupdate.txt`);
            const g = ((r.data as string).split(/[\n\s]/).filter(str => /\.gkg\.csv/i.test(str))[0] || '').trim();
            const e = ((r.data as string).split(/[\n\s]/).filter(str => /\.export\.csv/i.test(str))[0] || '').trim();
            const m = ((r.data as string).split(/[\n\s]/).filter(str => /\.mentions\.csv/i.test(str))[0] || '').trim();
            if (g) gkg = g;
            if (e) exportx = e;
            if (m) mentions = m;
        } catch (error) {
            Log.dev(error);

        }
        finally {
            resolve({ gkg, exportx, mentions });
        }
    });

    private static downloadZip = (url: string) => new Promise<any[] | null>(async (resolve, reject) => {
        try {
            Log.flow([SLUG, `Iteration`, `Downloading zip file from ${url}.`], WEIGHT);
            const response = await axios.get(url, { responseType: 'stream' });
            Log.flow([SLUG, `Iteration`, `Downloaded zip file. Unzipping`], WEIGHT);

            const unzipStream = response.data.pipe(unzipper.Parse());
            Log.flow([SLUG, `Iteration`, `Unzipped file`], WEIGHT);

            const rows: any[] = [];
            let entryCount = 0;

            unzipStream.on('entry', async (entry: any) => {
                const fileName = entry.path;
                const type = entry.type;

                if (type === 'File' && (fileName || '').toLowerCase().endsWith('.csv')) {
                    entryCount++;
                    const splitter = entry.pipe(split2());
                    const transformer = transform(
                        { parallel: 16 },
                        (line: string, cb: (err?: Error | null, data?: string) => void) => {
                            if (!line || line.trim() === '') return cb();
                            const cols = line.split('\t');
                            rows.push(cols);
                            cb();
                        }
                    );

                    await new Promise<void>((resolveEntry, rejectEntry) => {
                        splitter.pipe(transformer)
                            .on('data', () => {
                                if (MAX_RECORDS && rows.length >= MAX_RECORDS) {
                                    splitter.destroy();
                                    entry.destroy();
                                    resolveEntry();
                                }
                            })
                            .on('end', resolveEntry)
                            .on('error', rejectEntry);
                    });

                    entry.autodrain();
                    entryCount--;
                } else {
                    entry.autodrain();
                }
            });

            unzipStream.on('close', () => {
                if (entryCount === 0) {
                    resolve(rows);
                }
            });

            unzipStream.on('error', (e: any) => {
                Log.dev('Unzipstream error', e);
                reject(e);
            });
        } catch (error) {
            Log.dev(error);
            resolve(null);
        }
    });

    private static lastGKGURL: string = '';

    private static processGKGdata = (
        rows: string[][],
        cmap: Record<string, string>
    ) => {
        const d = new Date();
        const structured: Record<CountryCode, Record<
            string,
            RawData
        >> = {}
        const today = `${d.getFullYear()}${(d.getMonth() + 1).toString().padStart(2, '0')}${d.getDate().toString().padStart(2, '0')}`;
        for (const cols of rows) {
            if ((cols[1] || '').startsWith(today)) {
                const domainCT = ((cols[3] || "").split(".").slice(-1)[0] || '').toUpperCase();
                let country = "";
                if (!country) {
                    if (countryCodes.includes(domainCT)) {
                        country = domainCT;
                    }
                }
                if (!country) {
                    if (cmap[cols[4]]) {
                        country = cmap[cols[4]];
                    }
                }
                if (!country) {
                    const mentioned = arrayMode((cols[10] || '').split(';').map(x => x.split("#").filter(x => x.length > 0)[2]).filter(Boolean));
                    if (mentioned) {
                        country = mentioned as string;
                    }
                }
                const themes = (cols[7] || '').split(";").map(x => x.trim()).filter(Boolean);
                const category = theme2category(themes);
                if (country && category) {
                    const keywords = Array.from(new Set((cols[23] || '').split(";").map(x => x.split(",")[0].trim()).filter(Boolean)));
                    if (keywords.length > 0) {
                        const tone = parseFloat((cols[15] || '').split(",")[0]) || 0;
                        if(!structured[country]){
                            structured[country] = {};
                        }
                        for(const keyword of keywords){
                            if(structured[country][keyword]){
                                // keyword already exist in the country
                                // updating category
                                structured[country][keyword].category = Array.from(new Set(structured[country][keyword].category.concat([category])));
                                if(structured[country][keyword].category.length > MAX_PARSED_ITEMS){
                                    structured[country][keyword].category = structured[country][keyword].category.slice(structured[country][keyword].category.length - MAX_PARSED_ITEMS);
                                }
                                // finalizing
                                const newCount =  structured[country][keyword].count + 1;
                                structured[country][keyword].tone = (structured[country][keyword].tone * structured[country][keyword].count + tone) / newCount;
                                structured[country][keyword].count = newCount;
                            }
                            else if(Object.keys(structured[country]).length < MAX_PARSED_ITEMS){
                                // new keyword
                                structured[country][keyword] = {
                                    category: [category],
                                    count: 1,
                                    tone: tone,
                                }
                            }
                        }
                    }
                }
                // rows.push(cols);
            }
        }
        return structured;
    }

    private static run = async () => {
        const start = Date.now();
        Log.flow([SLUG, `Iteration`, `Initialized.`], WEIGHT);
        Log.flow([SLUG, `Iteration`, `Fetching latest GKG URL.`], WEIGHT);
        // BEGIN MAIN BODY
        const { gkg, exportx, mentions } = await MainEngine.getLatestGKGURL();
        if (gkg && exportx && mentions) {
            if (gkg == MainEngine.lastGKGURL) {
                Log.flow([SLUG, `Iteration`, `Warning`, `Duplicate GKG URL.`], WEIGHT);
            }
            else {
                Log.flow([SLUG, `Iteration`, `Success`, `Gotten new GKG URL.`], WEIGHT);
                const expData: Record<string, string> = Object.fromEntries((await MainEngine.downloadZip(exportx) || []).map(x => ([
                    x.slice(-1)[0],
                    countries3to2[x[7] || x[17]] || ''
                ])).filter(x => x[0] && x[1]));
                const gkgData = await MainEngine.downloadZip(gkg);
                if (gkgData) {
                    MainEngine.lastGKGURL = gkg;
                    const m = MainEngine.processGKGdata(gkgData, expData);
                    console.log(JSON.stringify(m, null, 2));
                    // TODO - Continue from here.
                    /**
                     * Now we have a structured data of updated trends by countries...
                     * we need extra config vars SOFT_TIMEOUT_MS (a few hours), and HARD_TIMEOUT_MS (a day or more)
                     * We need to maintain a static object that we will be consolidating this to. it should be initialized with an empty object for each country in the start method
                     * for each country present in new one
                     * * update static object and ensure it is sorted already in descending order of count.
                     * * remove soft expired keywords
                     * * then remove excess keywords that exceed MAX_PARSED_WHATEVER
                     * for other countries not present in new object.
                     * * remove hard expired keywords.
                     * ensure all keywords are sorted after any change, for easy retrieval and parsing, and snapshots.
                     * proposed structure of static object
                     * Record<CountryCode, {
                     *  keyword: string,
                     *  count: number,
                     *  categories: CategorySlug[]... an array of category slugs' the keyword is associated with... keeping only recent N (say 2 - 5),
                     *  note: number,
                     *  lastUpdated: number,
                     *  delta: number (+/- indicating its direction of movement and integer specifying the magnitude of its movement in that direction, if moved during sorting.... e.g. -2, for dropping down the list)
                     * }[]>
                     */
                }
                else {
                    Log.flow([SLUG, `Iteration`, `Error`, `Could not get GKG data.`], WEIGHT);
                }
            }
        }
        else {
            Log.flow([SLUG, `Iteration`, `Error`, `Failed to get GKG URL.`], WEIGHT);
        }
        // END MAIN BODY
        Log.flow([SLUG, `Iteration`, `Concluded.`], WEIGHT);
        const duration = Date.now() - start;
        if (duration >= Site.MAIN_INTERVAL_MS) {
            MainEngine.run();
        }
        else {
            setTimeout(() => {
                MainEngine.run();
            }, (Site.MAIN_INTERVAL_MS - duration));
            Log.flow([SLUG, `Iteration`, `Scheduled for ${getTimeElapsed(0, (Site.MAIN_INTERVAL_MS - duration))}.`], WEIGHT);
        }
    }
}