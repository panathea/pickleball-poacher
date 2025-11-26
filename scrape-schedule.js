import puppeteer from 'puppeteer-extra'
import StealthPlugin from 'puppeteer-extra-plugin-stealth'
import cheerio from 'cheerio'
import moment from 'moment'
import YAML from 'yaml'
import args from 'args'
import { readFile, writeFile } from 'fs'
import { promisify } from 'util'

const writeFileAsync = promisify(writeFile)
import fetch from 'node-fetch'

// Enable stealth plugin to avoid detection
puppeteer.use(StealthPlugin())

const centres = [
  'https://ottawa.ca/en/recreation-and-parks/facilities/place-listing/diane-deans-greenboro-community-centre',
  'https://ottawa.ca/en/recreation-and-parks/facilities/place-listing/francois-dupuis-recreation-centre',
  'https://ottawa.ca/en/recreation-and-parks/facilities/place-listing/heron-road-community-centre',
  'https://ottawa.ca/en/recreation-and-parks/facilities/place-listing/hintonburg-community-centre',
  'https://ottawa.ca/en/recreation-and-parks/facilities/place-listing/hunt-club-riverside-park-community-centre',
  'https://ottawa.ca/en/recreation-and-parks/facilities/place-listing/minto-recreation-complex-barrhaven',
  'https://ottawa.ca/en/recreation-and-parks/facilities/place-listing/nepean-sportsplex',
  'https://ottawa.ca/en/recreation-and-parks/facilities/place-listing/overbrook-community-centre',
  'https://ottawa.ca/en/recreation-and-parks/facilities/place-listing/pat-clark-community-centre',
  'https://ottawa.ca/en/recreation-and-parks/facilities/place-listing/richcraft-recreation-complex-kanata',
  'https://ottawa.ca/en/recreation-and-parks/facilities/place-listing/richelieu-vanier-community-centre',
  'https://ottawa.ca/en/recreation-and-parks/facilities/place-listing/rideauview-community-centre',
  'https://ottawa.ca/en/recreation-and-parks/facilities/place-listing/routhier-community-centre',
  'https://ottawa.ca/en/recreation-and-parks/facilities/place-listing/south-fallingbrook-community-centre',
]

// One week          day hr   min  sec  ms
const NEW_TIMESLOT = 7 * 24 * 60 * 60 * 1000
const defaultDays = [
  'Monday',
  'Tuesday',
  'Wednesday',
  'Thursday',
  'Friday',
  'Saturday',
  'Sunday',
]

const CAPTION_REGEX =
  /(starting|until|January|February|March|April|May|June|July|August|September|October|November|December)/i

const getPreviousTimes = async () => {
  return new Promise((resolve, reject) => {
    readFile('./cache/date-scraped.json', 'utf8', (err, data) => {
      if (err) reject(err)
      try {
        resolve(JSON.parse(data || '{}'))
      } catch (e) {
        resolve({})
      }
    })
  })
}
const getPreviousSchedule = async () => {
  return new Promise((resolve, reject) => {
    readFile('./cache/schedule.json', 'utf8', (err, data) => {
      if (err) reject(err)
      try {
        resolve(JSON.parse(data || '{}'))
      } catch (e) {
        resolve({})
      }
    })
  })
}

args
  .option('coordinates', 'Fetch coordinates of locations.', false)
  .option(
    'coordinates-only',
    'Only populate coordinates from existing schedule.json without scraping.',
    false
  )
  .option(
    'evenings-and-weekends',
    'Only return schedule of times in the evenings and weekends.',
    false
  )
  .option('debug', 'Log debug information.', false)
  .option('format', 'Output final list in JSON or YAML.', 'yaml', (value) => {
    if (value.startsWith('y')) return 'yaml'
    return 'json'
  })
  .option('outfile', 'File to write to, if not provided, log to console', '')

const flags = args.parse(process.argv)

const log = (...messages) => {
  if (!flags.debug) return
  console.log(...messages)
}

const eveningsAndWeekends = (day) => (time) => {
  if (/sat|sun/i.test(day)) return true

  const afternoon = time.includes('pm')
  if (!afternoon) return false

  const startTime = parseInt(time)
  return startTime !== 12 && startTime >= 5 && startTime < 10
}

function timeout(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

// Random delay between min and max milliseconds to mimic human behavior
function randomDelay(min, max) {
  return timeout(Math.floor(Math.random() * (max - min + 1)) + min)
}

// Initialize browser instance (reused across requests)
let browser = null

const getBrowser = async () => {
  if (!browser) {
    browser = await puppeteer.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-blink-features=AutomationControlled',
        '--disable-features=IsolateOrigins,site-per-process',
      ],
    })
  }
  return browser
}

const closeBrowser = async () => {
  if (browser) {
    await browser.close()
    browser = null
  }
}

// Fetch page content using Puppeteer with stealth mode
const fetchPageContent = async (url) => {
  const browserInstance = await getBrowser()
  const page = await browserInstance.newPage()

  try {
    // Set a realistic viewport
    await page.setViewport({ width: 1920, height: 1080 })

    // Set realistic user agent (stealth plugin handles this, but we can override)
    await page.setUserAgent(
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )

    // Navigate to the page
    await page.goto(url, { waitUntil: 'networkidle2' })
    log('Navigated to', url)

    // Add a small random delay to mimic human reading time
    await randomDelay(1500, 2000)
    log('Delayed')
    // Get the page content
    const content = await page.content()
    log('Got content')
    return content
  } finally {
    await page.close()
  }
}

const coordinatesCache = {}
const addressWithoutPostalCode = (address) =>
  address.split(/\s+/).slice(0, -2).join(' ')
const cacheOldCoordinates = async () => {
  const schedule = await getPreviousSchedule()
  Object.keys(schedule).forEach((location) => {
    let { address, coordinates } = schedule[location]
    address = addressWithoutPostalCode(address)
    if (coordinates?.lat) {
      coordinatesCache[address] = coordinates
    }
  })
}
const fetchCoordinates = async (address) => {
  // Remove postal code as OSM has many disagreements with the source.
  address = addressWithoutPostalCode(address)
  if (coordinatesCache[address]) {
    log('using coordinates cache')
    return coordinatesCache[address]
  }
  log('not using coordinates cache')
  const addressQuery = encodeURI(address.replace(/\s+/g, '+'))

  console.log('Fetching coordinates for', `https://nominatim.openstreetmap.org/search?q=${addressQuery}&format=jsonv2`)
  const response = await fetch(
    `https://nominatim.openstreetmap.org/search?q=${addressQuery}&format=jsonv2`,
    { headers: { 'User-Agent': 'PickleballScheduleScraper/0.1' } }
  )
  console.log('Response', response.status, response.statusText)

  // Rate limit is once per second.
  await timeout(1500)

  const addressDetails = await response.json()

  const { lat, lon } = addressDetails[0]

  coordinatesCache[address] = { lat, lon }
  return { lat, lon }
}

const populateCoordinatesOnly = async () => {
  try {
    await cacheOldCoordinates()
    
    const schedule = await getPreviousSchedule()
    log('Loaded schedule.json with', Object.keys(schedule).length, 'locations')
    
    let updated = false
    for (const locationName in schedule) {
      const location = schedule[locationName]
      const { address, coordinates } = location
      
      // Check if coordinates are missing or invalid (0,0)
      const needsCoordinates = !coordinates || 
                               !coordinates.lat || 
                               !coordinates.lon || 
                               (coordinates.lat === 0 && coordinates.lon === 0)
      
      if (needsCoordinates) {
        console.log(`Fetching coordinates for: ${locationName}`)
        console.log(`  Address: ${address}`)
        try {
          const newCoordinates = await fetchCoordinates(address)
          location.coordinates = newCoordinates
          updated = true
          console.log(`  Coordinates: ${newCoordinates.lat}, ${newCoordinates.lon}`)
        } catch (e) {
          console.error(`  Error fetching coordinates: ${e.message}`)
        }
      } else {
        log(`Coordinates already exist for: ${locationName}`)
      }
    }
    
    if (updated) {
      const outfile = flags.outfile || './cache/schedule.json'
      const { stringify } =
        flags.format === 'json' || outfile.endsWith('.json')
          ? { stringify: (value) => JSON.stringify(value, null, 2) }
          : YAML
      try {
        await writeFileAsync(outfile, stringify(schedule), 'utf8')
        console.log(`Updated ${outfile} with coordinates`)
      } catch (err) {
        console.error('Error writing file:', err)
        process.exit(1)
      }
    } else {
      console.log('All locations already have coordinates')
    }
  } catch (e) {
    console.error('Error in populateCoordinatesOnly:', e)
    process.exit(1)
  }
}

async function main() {
  try {
    // If coordinates-only mode, skip scraping and just populate coordinates
    if (flags.coordinatesOnly) {
      await populateCoordinatesOnly()
      return
    }

    await cacheOldCoordinates()

    log('About to scrape', centres.length, 'centres.')
    if (!centres.length) {
      process.exit(1)
    }

    const results = []
    for (let index = 0; index < centres.length; index++) {
      const centre = centres[index]

      // Add delay between requests to avoid rate limiting
      if (index > 0) {
        await randomDelay(1500, 4000)
      }

      log('Fetching page content for', centre)
      const htmlContent = await fetchPageContent(centre)
      const $ = cheerio.load(htmlContent)
      const location = $('h1').text().trim()
      const link = $('a:contains("Reserve")').attr('href')
      const home = centre
      const streetAddress = $('.address-link.address-details').text().replace(' (link is external)', '').trim()
      const addressDetails = $(
        '.address-link.address-details + .address-details'
      )
        .text()
        .trim()
      const address = `${streetAddress} ${addressDetails}`
        .split(/\s+/g)
        .join(' ')

      console.log('Address', address)
      console.log('Location', location)
      console.log('Link', link)
      console.log('Home', home)
      console.log('Street Address', streetAddress)
      console.log('Address Details', addressDetails)

      const activities = $('tr:contains("Pickleball")')
        .toArray()
        .map((element) => {
          const table = $(element).parents('table')
          const getDays = () => {
            const thead = $('thead tr th', table)
            const firstRow = $('tbody tr:first-of-type td', table)
            const dayRow = thead.text().includes('Monday')
              ? thead
              : firstRow.text().includes('Monday')
              ? firstRow
              : null
            if (!dayRow) return defaultDays
            return dayRow
              .toArray()
              .map((el) => $(el).text().trim())
              .filter((x) => x)
          }
          const days = getDays()
          let caption = table.find('caption').text()

          caption = CAPTION_REGEX.test(caption)
            ? caption.slice(caption.search(CAPTION_REGEX))
            : null
          const headName = $('th', element).text().replace(/\s+/g, ' ')
          const activityIsHead = !!headName
          const activity =
            headName ||
            $('td:first-of-type', element).text().replace(/\s+/g, ' ')

          const schedules = $('td', element)
            .toArray()
            .map((day, index) => {
              const actualIndex = index - (activityIsHead ? 0 : 1)
              const schedule = $(day)
                .text()
                .toLowerCase()
                .replace(/noon/g, '12 pm')
                .replace(/–/g, '-') // Remove endash.
                .replace(/([^ ])-([^ ])/g, '$1 - $2') // Ensure spaces around time.
                .split(/(,|\n+|,)/)
                .map((time) => time.trim())
                .filter((time) => !isNaN(parseInt(time)))
                .filter(
                  flags.e ? eveningsAndWeekends(days[actualIndex]) : () => true
                )
              return { day: days[actualIndex], schedule }
            })
            .reduce((result, current) => {
              if (current.schedule.length) {
                result[current.day] = current.schedule
              }
              return result
            }, {})
          return {
            location: [location, caption].filter((x) => x).join(' '),
            link,
            home,
            address,
            activity,
            schedules,
          }
        })
        .filter((x) => JSON.stringify(x.schedules) !== '{}')
      console.log('Activities', activities)
      results.push(...activities)
    }

    if (flags.coordinates) {
      for (const activitySchedule of results) {
        const coordinates = await fetchCoordinates(activitySchedule.address)
        activitySchedule.coordinates = coordinates
      }
    }

    const resultsByLocation = results.reduce((acc, activitySchedule) => {
      const location = {
        link: activitySchedule.link,
        home: activitySchedule.home,
        address: activitySchedule.address,
        coordinates: activitySchedule.coordinates,
      }
      defaultDays.forEach((day) => {
        const daySchedule = [
          ...(acc[activitySchedule.location]?.[day] || []),
          ...(activitySchedule.schedules[day] || []).map(
            // Fix `2: 45 pm` --> `2:45 pm`
            (time) =>
              `${time.replace(
                / ?: ?/g,
                ':'
              )} (${activitySchedule.activity.trim()})`
          ),
        ].sort((a, b) => {
          const getTime = (x) =>
            moment(
              x
                .split(/[–-]/)[1]
                .substring(0, Math.max(x.indexOf('am'), x.indexOf('pm') + 2))
                .trim(),
              ['h:mm a', 'h a']
            )
          return getTime(a) - getTime(b)
        })
        if (daySchedule.length) {
          location[day] = daySchedule
        }
      })
      acc[activitySchedule.location] = location
      return acc
    }, {})

    const { stringify } =
      flags.format === 'json'
        ? { stringify: (value) => JSON.stringify(value, null, 2) }
        : YAML
    const newTimes = buildDateTable(await getPreviousTimes(), resultsByLocation)
    for (const key in newTimes) {
      if (Date.now() - newTimes[key] < NEW_TIMESLOT) {
        log('new!')
        const [shortLocation, day, time] = key.split('|')
        Object.keys(resultsByLocation).forEach((location) => {
          if (!location.includes(shortLocation))
            return log('does not include short')
          if (!resultsByLocation[location]?.[day]) return log('no day...')
          resultsByLocation[location][day] = resultsByLocation[location][
            day
          ].map((startEnd) => {
            if (startEnd.startsWith(time)) return `${startEnd}*`
            return startEnd
          })
        })
      }
    }
    writeFile(
      './cache/date-scraped.json',
      JSON.stringify(newTimes, null, 2),
      'utf8',
      () => {}
    )
    if (flags.outfile) {
      writeFile(flags.outfile, stringify(resultsByLocation), 'utf8', () => {})
    } else {
      console.log(stringify(resultsByLocation))
    }

    // Close browser when done
    await closeBrowser()
  } catch (e) {
    console.error(e)
    // Ensure browser is closed even on error
    await closeBrowser()
    process.exit(1)
  }
}

/**
 * Store the time and location pair along with the date it was first scraped. This will make highlighting new entries possible.
 */
export function buildDateTable(previous, current) {
  const result = {}
  for (const locationName in current) {
    const captionIndex = locationName.search(CAPTION_REGEX)
    const name = locationName.slice(0, captionIndex).trim()
    const location = current[locationName]
    for (const day of defaultDays) {
      const times = location[day]
      if (!times) continue
      for (const time of times) {
        const timeWithoutActivity = time.split(' (')[0]
        const key = `${name}|${day}|${timeWithoutActivity}`
        const olderDate = previous[key] || Date.now()
        result[key] = olderDate
      }
    }
  }
  return result
}

main()
