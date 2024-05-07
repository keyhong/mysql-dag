import pytest
import configparser

from config_reader import ConfigReader

@pytest.fixture
def config_reader_fixture():
    config_reader = ConfigReader()
    yield config_reader

def test_get_value(config_reader_fixture):
    sample_data = {"section_name": "CHECK_OPTION", "option_name": "CHECK_CYCLE", "response": "5"}
    
    response = config_reader.get_value(sample_data["section_name"], sample_data["option_name"])

    assert sample_data["response"] == response, f"Response does not match. {sample_data["response"]}, {response} is diff."

def test__get_value__no_section_name(config_reader_fixture):

    import uuid
    
    sample_data = {"section_name": uuid.uuid4().hex, "option_name": "CHECK_CYCLE"}

    with pytest.raises(configparser.NoSectionError):
        response = config_reader.get_value(sample_data["section_name"], sample_data["option_name"])

def test__get_value__no_option_name(config_reader_fixture):

    sample_data = {"section_name": "CHECK_OPTION", "option_name": "CHECK_CYCLE", "response": "a"}
    
    with pytest.raises(configparser.NoOptionError):
        response = config_reader.get_value(sample_data["section_name"], sample_data["option_name"])