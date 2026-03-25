
PORT = 7778

# Site address used in email links (protocol, host and base URL)
# Change SITE_HOST to the hostname or IP your users should open (e.g. 'localhost' or 'example.com')


SECRET_KEY = """Far over the misty mountains cold
                To dungeons deep and caverns old
                We must away, ere break of day
                To seek our pale enchanted gold
                The dwarves of yore made mighty spells
                While hammers fell like ringing bells
                In places deep, where dark things sleep
                In hollow halls beneath the fells
                For ancient king and elvish lord
                There many a gleaming golden hoard
                They shaped and wrought, and light they caught
                To hide in gems on hilt of sword
                On silver necklaces they strung
                The flowering stars on crowns they hung
                The dragon-fire in twisted wire
                They meshed the light of moon and sun"""

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440 * 7

SCHEMES = "argon2"

JWE_ENC_PASSWORD =   """Come ye Dwarves of Hammerdeep,
                        Our home was burned by dragonfire,
                        For seven days these seven beasts,
                        We fought with Dwarven ire.
                        We fought and made them flee,
                        We sail to pay the score,
                        Dwarves detest the sea,
                        We hate these dragons more!"""  # 32 байта для A256GCM
JWE_ALG = "dir"
JWE_ENC = "A256GCM"