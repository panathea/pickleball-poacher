import React from 'react'
import { addDistance, days, locationsThatHaveDay, today } from './schedule'
import styles from './ScheduleForDay.module.css'
import { getEndTime, dayjs, parseDay } from './time'
import { TimeFormatter } from './TimeFormatter'
import { useRefresh } from './useRefresh'

const CAPTION_REGEX =
  /(starting|until|January|February|March|April|May|June|July|August|September|October|November|December|#)/i

const getCaptionDateRange = (caption = '') => {
  // After: starting December 12
  // Before: until December 12
  // Range: December 12 to January 2
  const variant = caption.includes(' to ')
    ? 'range'
    : caption.includes('starting')
    ? 'from'
    : caption.includes('until')
    ? 'to'
    : 'none'

  if (variant === 'from') {
    const date = caption.split('starting')[1].trim()
    return { from: parseDay(date, false), to: null }
  }
  if (variant === 'to') {
    const date = caption.split('until')[1].trim()
    return { from: null, to: parseDay(date, true) }
  }
  if (variant === 'range') {
    let [fromDate, toDate] = caption.split(' to ').map((x) => x.trim())
    // Handle "December 1 to 10" format.
    if (toDate.length <= 2) {
      toDate = `${fromDate.split(' ')[0]} ${toDate}`
    }
    return { from: parseDay(fromDate, false), to: parseDay(toDate, true) }
  }
  return { from: null, to: null }
}

export default function ScheduleForDay({
  day,
  daysAway,
  latitude,
  longitude,
  now,
}) {
  useRefresh()
  const isToday = day === today
  const then = dayjs(now).add(daysAway, 'day')
  const locationsWithDistance = addDistance(
    locationsThatHaveDay(day),
    latitude,
    longitude,
    true
  )
  const warnRegistration = daysAway > 2 || (daysAway === 2 && now.hour() < 18)
  const registrationDay = days[(days.indexOf(day) + 7 - 2) % 7]
  const isWeekend = day === 'Saturday' || day === 'Sunday'
  return (
    <div
      id={day}
      className={`day-card ${styles.card} ${isWeekend ? styles.weekend : ''}`}
    >
      <h2>{then.format('dddd, MMMM D')} in Ottawa</h2>
      {warnRegistration && (
        <div>
          *Registration only opens up 2 days before the event, at 6 p.m.
        </div>
      )}
      <div className={styles.container}>
        {locationsWithDistance.map((location) => {
          const captionIndex = location.name.search(CAPTION_REGEX)
          const name = location.name.slice(0, captionIndex)
          const caption = location.name.slice(captionIndex).replace('#', '')
          const { from, to } = getCaptionDateRange(caption)

          // Exclude dates that aren't inclusive.
          if (from && then.isBefore(from)) return null
          if (to && then.isAfter(to)) return null

          const fullCaption = caption ? ` (${caption.trim()})` : null
          return (
            <div key={location.name} className={styles.location}>
              <h3>{name}</h3>
              {location.distance && (
                <p className={styles.distance}>
                  {location.distance.toFixed(1)} km
                </p>
              )}
              <note className={styles.caption}>
                {day}
                {fullCaption}
              </note>
              <ul>
                {location[day].map((time) => {
                  const { past, inProgress } = (() => {
                    if (!isToday) return { past: false, inProgress: false }
                    const endTime = getEndTime(time)
                    const past = endTime.isBefore(now)
                    const inProgress =
                      !past && endTime.isBefore(now.add(1, 'hour'))
                    return {
                      past,
                      inProgress,
                    }
                  })()
                  const className = `${inProgress ? styles.inprogress : ''} ${
                    past ? styles.past : ''
                  }`
                  return (
                    <li key={time + className} className={className}>
                      <TimeFormatter time={time} day={day} />
                      {/* This div forces an update which fixes a bug with SSR where time styles are no longer dynamic. */}
                      <div style={{ display: 'none' }}>{className}</div>
                    </li>
                  )
                })}
              </ul>
              <a
                className={styles.button}
                href={location.link}
                rel="noreferrer noopener"
                target="_blank"
              >
                Register{' '}
                {warnRegistration && !location.name.includes('RA Centre') && !location.name.includes('OFSL') && (
                  <>
                    at 6 pm{' '}
                    {registrationDay === today ? 'today' : registrationDay}*
                  </>
                )}
              </a>
              <a
                className={styles.button}
                href={`https://google.com/maps?q=${location.address.replace(
                  /\s+/g,
                  '+'
                )}`}
                rel="noreferrer noopener"
                target="_blank"
              >
                Directions
              </a>
              <a
                className={styles.button}
                href={location.home}
                rel="noreferrer noopener"
                target="_blank"
              >
                Homepage
              </a>
            </div>
          )
        })}
      </div>
    </div>
  )
}
