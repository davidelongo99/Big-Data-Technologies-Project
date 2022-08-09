import webbrowser

def urlopen(url1):
    webbrowser.open(url1)


command = input("Choose: general, international, services, quality .").lower()
# This way we can select the chosen parameter, while not being uppercase sensitive.

if command == 'general':
    urlopen('https://datastudio.google.com/reporting/08e06bfa-aec0-474b-8998-15791b4140ac')
elif command == 'international':
    urlopen('https://datastudio.google.com/reporting/c0912393-9e85-4097-a7cc-94a0ea40cd6a')
elif command == 'services':
    urlopen('https://datastudio.google.com/reporting/99d44256-d1e6-432a-aea0-bc86dace4c90')
elif command == 'quality':
    urlopen('https://datastudio.google.com/reporting/f0250ab0-d94f-4cb4-b2e1-66198b98ff7b')
else:
    print("Error! Ranking requested unavailable")

urlopen(url)  # Open desired page.
