import time

from flask import current_app
from python_anticaptcha import AnticaptchaClient, NoCaptchaTaskProxylessTask


def get_token(url, site_key, invisible, client, timeout):
    task = NoCaptchaTaskProxylessTask(
        website_url=url, website_key=site_key, is_invisible=invisible
    )
    job = client.createTask(task)
    try:
        job.join(maximum_time=timeout)
    except Exception:
        return False
    return job.get_solution_response()


def form_submit(driver, token):
    driver.execute_script(
        "document.getElementById('g-recaptcha-response').innerHTML='{}';".format(token)
    )
    time.sleep(1)
    driver.execute_script(
        "document.getElementsByClassName('captcha_block_form')[0].submit()"
    )
    time.sleep(1)


def get_sitekey(driver):
    return driver.find_element_by_class_name("g-recaptcha").get_attribute(
        "data-sitekey"
    )


def process_captcha(driver):
    api_key = current_app.config["ANTICAPTCHA_TOKEN"]
    url = driver.current_url
    client = AnticaptchaClient(api_key)
    invisible_captcha = True

    site_key = get_sitekey(driver)
    print("Found site-key", site_key)
    token = get_token(url, site_key, invisible_captcha, client, current_app.config["ANTICAPTCHA_TIMEOUT"])
    if not token:
        return False
    print("Found token", token)
    form_submit(driver, token)
    return True
