#!/usr/bin/env python3
"""
Domain Blocking Module
Contains lists of blocked domains and patterns for filtering out problematic emails.
These are typically agency/management company emails, not property owner emails.
"""

# Blocked domains (exact matches)
BLOCKED_DOMAINS = [
    'holidu.com',
    'awaze.com',
    'belvilla.com',
    'bestfewo.de',
    'e-domizil.de',
    'secra.de',
    'homerti.com',  # Management company
    'landfolk.com',  # Management company
    'placeyourplace.es',  # Management company
    'staymoovers.com',  # Management company
    'gastgeberservice.com',  # Management company
    'villaforyou.com',  # Management company (Villa for You)
    'ehlen-erdbohrungen.com',  # Blocked domain
    'die-fewoagentur.de',  # Blocked domain
    # Additional blocked domains from email list
    'hauswohnungmeer.de',
    'kuestenimmobilien.com',
    'obsg.de',
    'kraushaar-ferienwohnungen.de',
    'urlaubsservice-eiderstedt.de',
    'ferienhausvermittlung-fehmarn.de',
    'stolz.ruegen-resort-sagard.de',
    'albatross.de',
    'dancenter.com',
    'ferienhaeuser-hasselfelde.de',
    'travanto.de',
    'baltic-appartements.de',
    'aller-leine-tal.de',
    'strandperlen.de',
    'pas-poel.de',
    'ds-destinationsolutions.com',
    'v-office.com',
    'acontour.de',
    'usedomliebe.de',
    'urlaubanderostsee.de',
    # Major European vacation rental management companies
    'interhome.group',  # Interhome - European holiday-home specialist
    'interhome.com',  # Interhome alternative domain
    'novasol.com',  # NOVASOL - European vacation rental provider
    'novasol.dk',  # NOVASOL alternative domain
    'guestready.com',  # GuestReady - Property management company
    'homerez.com',  # Homerez - Short-term rental management
    'helloguest.com',  # HelloGuest - UK rental management
    'helloguest.co.uk',  # HelloGuest UK domain
    'plumguide.com',  # The Plum Guide
    'sykescottages.co.uk',  # Sykes Holiday Cottages
    'sykescottages.com',  # Sykes Holiday Cottages alternative
    'lomarengas.com',  # Lomarengas
    'esmark.com',  # Esmark
    'italianway.com',  # Italianway
    'bungalownet.com',  # BungalowNet
    'altido.com',  # Altido
    'happyholidayhomes.com',  # Happy Holiday Homes
    'cityrelay.com',  # City Relay
    'hostnfly.com',  # Hostnfly
    'houst.com',  # Houst
    'onefinestay.com',  # Onefinestay
    'halldis.com',  # Halldis
    'friendlyrentals.com',  # Friendly Rentals
    'travelopo.com',  # Travelopo
    'passthekeys.com',  # Pass the Keys
    'nestify.com',  # Nestify
    'hyatus.com',  # Hyatus Stays
    'vacasa.com',  # Vacasa
    'hostmaker.com',  # Hostmaker
    'vtrips.com',  # VTrips
    'ruralidays.com',  # Ruralidays
    'bnblord.com',  # BnbLord
    'verdeaturismo.com',  # Verdea Asesores Turisticos - Management company
    # Suspicious property management domains (from analysis)
    'globalcc-ltd.com',  # Corporate entity (LTD = Limited Company)
    'alicante-realestate.com',  # Real estate company domain
    'nuahotels.com',  # Hotel chain/management company
    'resorthosts.com',  # Property management/resort hosting company
    'amazzzing.com',  # Travel/management company
    'qualitysystem.de',  # Business management system
    'homestobe.com',  # Property management platform
    'hrl-projekt.de',  # "Projekt" = project/development company
    'zapholiday.es',  # Holiday management company
    'foehrreisen.de',  # "Reisen" = travel agency
    'hotel-select.de',  # Hotel management company
    'chelo.com',  # Generic business domain
    'aspasios.com',  # Possible property management company
    'lunavillas.es',  # Could be villa management company
    'traumferienhaus-sauerland.de',  # Could be property management
    'gg-fed.de',  # Business domain
    'cateva.es',  # Business domain
    # Additional suspicious domains from CSV analysis
    'cerdanyabnb.com',  # BNB management company
    'nerjaholidayrentals.com',  # Holiday rental management
    'beac-realestate.com',  # Real estate company
    'villalia.es',  # Villa management company (villas plural)
    # Italian travel/rental/management companies
    'italianway.com',  # Italianway - already listed above but keeping for reference
    'halldis.com',  # Halldis - already listed above but keeping for reference
    'friendlyrentals.com',  # Friendly Rentals - already listed above but keeping for reference
    'travelopo.com',  # Travelopo - already listed above but keeping for reference
    'homeaway.it',  # HomeAway Italy (now Vrbo)
    'vrbo.it',  # Vrbo Italy
    'bookingpalace.it',  # Booking management company
    'rentalmanagement.it',  # Rental management company
    'gestioneaffitti.it',  # Property management (gestione = management, affitti = rentals)
    'affittibrevi.it',  # Short-term rental management
    'holidayrentalsitaly.com',  # Holiday rentals Italy
    'italianvillas.com',  # Italian villas management
    'italyrentals.com',  # Italy rentals
    'tuscanynow.com',  # Tuscany property management
    'thinkitaly.com',  # Italy travel/rental company
    'italianvillas.net',  # Italian villas
    'italianholidayhomes.com',  # Italian holiday homes
    'italyperfect.com',  # Italy property management
    'rentvillas.com',  # Rent Villas - international but strong in Italy
    'thethinkingtraveller.com',  # Luxury villa rentals (Italy focus)
    'casa.it',  # Italian real estate portal
    'immobiliare.it',  # Italian real estate portal
    'idealista.it',  # Italian real estate portal
    # Croatian travel/rental/management companies
    'adriagate.com',  # Adriagate - Croatian travel agency/vacation rentals
    'adriaticluxury.com',  # Adriatic luxury rentals (Croatia)
    'adriatic.hr',  # Adriatic Croatia domain
    'croatia-villas.com',  # Croatia villas
    'croatiaholidays.com',  # Croatia holidays
    'croatia-rentals.com',  # Croatia rentals
    'croatia-vacation.com',  # Croatia vacation rentals
    'adriaticapartments.com',  # Adriatic apartments (Croatia)
    'istria-rentals.com',  # Istria rentals (Croatia region)
    'dalmatia-rentals.com',  # Dalmatia rentals (Croatia region)
    'croatia-accommodation.com',  # Croatia accommodation
    'croatia-villas.hr',  # Croatia villas .hr domain
    'rentcroatia.com',  # Rent Croatia
    'croatiaholidayrentals.com',  # Croatia holiday rentals
    'adriaticluxuryvillas.com',  # Adriatic luxury villas
    'croatia-vacation-rentals.com',  # Croatia vacation rentals
    'istria-villas.com',  # Istria villas
    'dalmatia-villas.com',  # Dalmatia villas
    'croatia-property.com',  # Croatia property
    'adriaticapartments.hr',  # Adriatic apartments .hr
    'apartments.hr',  # Croatian rental/management
    'aunaselect.com',  # Management company
    'bestofcrotia.eu',  # Management company
    'buqezecoresort.com',  # Management company
    # just-in-case exclusions
    # Major booking platforms and OTAs (Online Travel Agencies)
    'booking.com',  # Booking.com - major OTA
    'booking.de',  # Booking.com Germany
    'booking.fr',  # Booking.com France
    'booking.it',  # Booking.com Italy
    'booking.es',  # Booking.com Spain
    'booking.pl',  # Booking.com Poland
    'booking.hr',  # Booking.com Croatia
    'airbnb.com',  # Airbnb - vacation rental platform
    'airbnb.de',  # Airbnb Germany
    'airbnb.fr',  # Airbnb France
    'airbnb.it',  # Airbnb Italy
    'airbnb.es',  # Airbnb Spain
    'airbnb.pl',  # Airbnb Poland
    'airbnb.co.uk',  # Airbnb UK
    'expedia.com',  # Expedia - travel booking platform
    'expedia.de',  # Expedia Germany
    'expedia.it',  # Expedia Italy
    'expedia.co.uk',  # Expedia UK
    'tripadvisor.com',  # TripAdvisor - travel review/booking
    'tripadvisor.it',  # TripAdvisor Italy
    'tripadvisor.de',  # TripAdvisor Germany
    'tripadvisor.fr',  # TripAdvisor France
    'tripadvisor.es',  # TripAdvisor Spain
    'tripadvisor.co.uk',  # TripAdvisor UK
    'vrbo.com',  # Vrbo - vacation rental platform (formerly HomeAway)
    'homeaway.com',  # HomeAway - vacation rental (now Vrbo)
    'homeaway.co.uk',  # HomeAway UK
    'homeaway.de',  # HomeAway Germany
    'homeaway.fr',  # HomeAway France
    'homeaway.es',  # HomeAway Spain
    'agoda.com',  # Agoda - hotel booking platform
    'hotels.com',  # Hotels.com - hotel booking
    'hotels.de',  # Hotels.com Germany
    'hotels.it',  # Hotels.com Italy
    'trivago.com',  # Trivago - hotel metasearch
    'trivago.de',  # Trivago Germany
    'trivago.it',  # Trivago Italy
    'trivago.co.uk',  # Trivago UK
    'prontohotel.com',  # ProntoHotel - Italian hotel metasearch
    'prontohotel.it',  # ProntoHotel Italy
    # Travel agencies and tour operators
    'cit.it',  # Compagnia Italiana Turismo (CIT) - Italian travel agency
    'tui.com',  # TUI - major European travel agency
    'tui.de',  # TUI Germany
    'tui.co.uk',  # TUI UK
    'tui.it',  # TUI Italy
    'tui.es',  # TUI Spain
    'thomascook.com',  # Thomas Cook - travel agency
    'thomascook.co.uk',  # Thomas Cook UK
    'thomascook.de',  # Thomas Cook Germany
    'lastminute.com',  # Lastminute.com - travel deals
    'lastminute.de',  # Lastminute Germany
    'lastminute.it',  # Lastminute Italy
    'lastminute.co.uk',  # Lastminute UK
    'opodo.com',  # Opodo - online travel agency
    'opodo.de',  # Opodo Germany
    'opodo.it',  # Opodo Italy
    'opodo.co.uk',  # Opodo UK
    'edreams.com',  # eDreams - travel booking
    'edreams.de',  # eDreams Germany
    'edreams.it',  # eDreams Italy
    'edreams.es',  # eDreams Spain
    'listotravel.com',  # Listo Travel - Croatian travel agency
    'sail-croatia.com',  # Sail Croatia - Croatian cruise/travel agency
    'visitcroatia.com',  # Visit Croatia - travel/tourist information
    'travelocroatia.com',  # Exploring Tourism Croatia - travel agency
    'uhpa.hr',  # Association of Croatian Travel Agencies
    # Tourist information and visitor bureaus
    'croatia.hr',  # Croatian National Tourist Board
    'italia.it',  # Italian National Tourist Board (ENIT)
    'visititaly.it',  # Visit Italy - tourist information
    'italiantourism.com',  # Italian tourism information
    'italiantouristboard.com',  # Italian tourist board
    'croatiatraveller.com',  # Croatia Traveller - tourist information
    'croatia-tourism.com',  # Croatia tourism information
    'istria.hr',  # Istria Tourist Board
    'visit-istria.com',  # Visit Istria - tourist information
    'visit-dalmatia.com',  # Visit Dalmatia - tourist information
    'visit-venice.com',  # Visit Venice - tourist information
    'visit-tuscany.com',  # Visit Tuscany - tourist information
    'visit-rome.com',  # Visit Rome - tourist information
    'visit-florence.com',  # Visit Florence - tourist information
    'visit-milan.com',  # Visit Milan - tourist information
    # Poland - official tourist information sites
    'warsawtour.pl',  # Warsaw Tourist Office
    'go2warsaw.pl',  # Warsaw tourism
    'krakow.travel',  # Kraków tourism
    'visitgdansk.com',  # Gdańsk tourism
    'visitwroclaw.eu',  # Wrocław tourism
    'poznan.travel',  # Poznań tourism
    'poland.travel',  # Poland National Tourism
    # Spain - official tourist information sites
    'esmadrid.com',  # Madrid tourism
    'barcelonaturisme.com',  # Barcelona tourism
    'visitasevilla.es',  # Seville tourism
    'visitvalencia.com',  # Valencia tourism
    'bilbaoturismo.net',  # Bilbao tourism
    'spain.info',  # Spain National Tourism
    # Germany - official tourist information sites
    'visitberlin.de',  # Berlin tourism
    'muenchen.travel',  # Munich tourism
    'hamburg-travel.com',  # Hamburg tourism
    'frankfurt-tourismus.de',  # Frankfurt tourism
    'cologne-tourism.com',  # Cologne tourism
    'hamburg.com',  # Hamburg tourism
    'germany.travel',  # Germany National Tourism
    # Croatia - official tourist information sites
    'infozagreb.hr',  # Zagreb tourism
    'tzdubrovnik.hr',  # Dubrovnik Tourist Board
    'visitsplit.com',  # Split tourism
    'visitrijeka.hr',  # Rijeka tourism
    'zadar.travel',  # Zadar tourism
    # Italy - official tourist information sites
    'turismoroma.it',  # Rome tourism
    'yesmilano.it',  # Milan tourism
    'veneziaunica.it',  # Venice tourism
    'firenzeturismo.it',  # Florence tourism
    'italia.it',  # Italy National Tourism (already listed above but keeping)
    # Additional rental management platforms
    '9flats.com',  # 9flats - vacation rental platform
    '9flats.de',  # 9flats Germany
    '9flats.it',  # 9flats Italy
    'wimdu.com',  # Wimdu - vacation rental platform
    'wimdu.de',  # Wimdu Germany
    'wimdu.it',  # Wimdu Italy
    'housetrip.com',  # HouseTrip - vacation rental (now closed but domain may still be used)
    'housetrip.co.uk',  # HouseTrip UK
    'flipkey.com',  # FlipKey - vacation rental (TripAdvisor)
    'flipkey.co.uk',  # FlipKey UK
    'flipkey.de',  # FlipKey Germany
    'holidaylettings.co.uk',  # Holiday Lettings - vacation rental (TripAdvisor)
    'holidaylettings.com',  # Holiday Lettings
    'holidaylettings.de',  # Holiday Lettings Germany
    'holidaylettings.fr',  # Holiday Lettings France
    'holidaylettings.it',  # Holiday Lettings Italy
    'holidaylettings.es',  # Holiday Lettings Spain
    'ownersdirect.co.uk',  # Owners Direct - vacation rental
    'ownersdirect.com',  # Owners Direct
    'ownersdirect.de',  # Owners Direct Germany
    'ownersdirect.fr',  # Owners Direct France
    'ownersdirect.it',  # Owners Direct Italy
    'hometogo.com',  # HomeToGo - vacation rental metasearch
    'hometogo.de',  # HomeToGo Germany
    'hometogo.it',  # HomeToGo Italy
    'hometogo.co.uk',  # HomeToGo UK
    'hometogo.fr',  # HomeToGo France
    'hometogo.es',  # HomeToGo Spain
    'hometogo.pl',  # HomeToGo Poland
    'hometogo.hr',  # HomeToGo Croatia
    'rentalsunited.com',  # Rentals United - channel manager for rentals
    'rentalsunited.de',  # Rentals United Germany
    'rentalsunited.it',  # Rentals United Italy
    'kayak.com',  # Kayak - travel metasearch
    'kayak.de',  # Kayak Germany
    'kayak.it',  # Kayak Italy
    'kayak.co.uk',  # Kayak UK
    'skyscanner.com',  # Skyscanner - travel metasearch
    'skyscanner.de',  # Skyscanner Germany
    'skyscanner.it',  # Skyscanner Italy
    'skyscanner.co.uk',  # Skyscanner UK
    'momondo.com',  # Momondo - travel metasearch
    'momondo.de',  # Momondo Germany
    'momondo.it',  # Momondo Italy
    'momondo.co.uk',  # Momondo UK
    'contessa-villas.com',  # Rental/management company
    'go-to-travel.hr',  # Croatian rental/management
    'hostelier.eu',  # Management company
    'istra-vacation.com',  # Management company
    'luxevillascollection.com',  # Rental/management company
    'palazzovillanilubelli.com',  # Rental/management company
    'pam-villas.com',  # Rental/management company
    'rentistra.com',  # Management company
    'rentistria.com',  # Management company
    'ri.t-com.hr',  # Croatian rental/management
    'stayfritz.com',  # Management company
    'touristra.hr',  # Croatian rental/management
    'ullitravel.com',  # Management company
    'vbvillas.eu',  # Rental/management company
    'villas-guide.com',  # Rental/management company
    # Add more blocked domains here
]

# Blocked domain patterns (partial matches)
BLOCKED_DOMAIN_PATTERNS = [
    # Typo domains
    'gmail.pl',  # should be gmail.com
    'gimail.com',  # typo
    'hmail.com',   # typo 
    'gmai.com',    # typo
    
    # Invalid domains
    'end2endservice.de',
    'blauersee-gabsen.de',
    'opoczta.pl',
    
    # Domain patterns that should be blocked
    'booking.com@holidu',
    'novasol.booking',
    # Add more patterns here (will match if domain contains this string)
]

# Blocked email patterns (specific email addresses or patterns)
BLOCKED_EMAIL_PATTERNS = [
    'novasol.booking.com@awaze.com',
    'cs.bookingcom@holidu.com',
    'lhs-booking@holidu.com',
    'bookingservice@secra.de',
    'lhs-booking@holidu.com',
    'partnerprogramm@e-domizil.de',
    'service.fh@belvilla.com',
    'belvillapt@belvilla.com',
    'booking.com@bestfewo.de',
    'n/a',  # Block invalid "n/a" emails
    # Add more specific email patterns here
]

def is_domain_blocked(email: str) -> tuple[bool, str]:
    """
    Check if an email domain should be blocked
    
    Returns:
        Tuple of (is_blocked: bool, reason: str)
        - If blocked: (True, reason_string)
        - If not blocked: (False, '')
    """
    if not email:
        return False, ''
    
    email_lower = email.lower().strip()
    
    # Check for invalid "n/a" emails first
    if email_lower == 'n/a' or email_lower == 'na':
        return True, 'blocked_email_pattern:n/a'
    
    # Extract domain from email
    if '@' not in email_lower:
        return False, ''
    
    domain = email_lower.split('@')[1]
    
    # Check exact domain match
    if domain in BLOCKED_DOMAINS:
        return True, f"blocked_domain:{domain}"
    
    # Check domain patterns
    for pattern in BLOCKED_DOMAIN_PATTERNS:
        if pattern in domain:
            return True, f"blocked_pattern:{pattern}"
    
    # Check email patterns
    for pattern in BLOCKED_EMAIL_PATTERNS:
        if pattern.lower() in email_lower:
            return True, f"blocked_email_pattern:{pattern}"
    
    return False, ''


def get_domain_blocking_stats() -> dict:
    """Get statistics about domain blocking rules"""
    return {
        "blocked_domains_count": len(BLOCKED_DOMAINS),
        "blocked_email_patterns_count": len(BLOCKED_EMAIL_PATTERNS),
        "blocked_domain_patterns_count": len(BLOCKED_DOMAIN_PATTERNS),
        "total_rules": len(BLOCKED_DOMAINS) + len(BLOCKED_EMAIL_PATTERNS) + len(BLOCKED_DOMAIN_PATTERNS)
    }
