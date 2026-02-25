# Position Description Gaps

Positions that currently have no description in `essentials.position_descriptions`.
These need to be populated via the admin API (`POST /essentials/admin/position-descriptions`)
or direct SQL inserts.

## Have Descriptions (4)
- Mayor (LOCAL_EXEC)
- District Attorney (COUNTY)
- Governor (STATE_EXEC)
- Lieutenant Governor (STATE_EXEC)

## Missing: Local (4)
| Position | District Type |
|----------|--------------|
| Assessor | COUNTY |
| Sheriff | COUNTY |
| Council Member | LOCAL |
| Supervisor | LOCAL |

## Missing: School (1)
| Position | District Type |
|----------|--------------|
| Board Member | SCHOOL |

## Missing: State (19)
| Position | District Type |
|----------|--------------|
| Assembly Member | STATE_LOWER |
| Senator | STATE_UPPER |
| Attorney General | STATE_EXEC |
| Auditor | STATE_EXEC |
| Board of Equalization Member | STATE_EXEC |
| Chairman | STATE_EXEC |
| Commissioner of Insurance | STATE_EXEC |
| Commissioner of Labor | STATE_EXEC |
| Controller | STATE_EXEC |
| Director of Agriculture | STATE_EXEC |
| Director of Industrial Relations | STATE_EXEC |
| Director of Natural Resources | STATE_EXEC |
| Insurance Commissioner | STATE_EXEC |
| Public Utilities Commissioner | STATE_EXEC |
| Secretary for Natural Resources | STATE_EXEC |
| Secretary of Agriculture | STATE_EXEC |
| Secretary of Education | STATE_EXEC |
| Secretary of State | STATE_EXEC |
| Superintendent | STATE_EXEC |
| Treasurer | STATE_EXEC |
| Utility Regulatory Commissioner | STATE_EXEC |

## Missing: Federal (23)
| Position | District Type |
|----------|--------------|
| Representative | NATIONAL_LOWER |
| Senator | NATIONAL_UPPER |
| President | NATIONAL_EXEC |
| Vice President | NATIONAL_EXEC |
| Attorney General | NATIONAL_EXEC |
| Secretary of Agriculture | NATIONAL_EXEC |
| Secretary of Commerce | NATIONAL_EXEC |
| Secretary of Defense | NATIONAL_EXEC |
| Secretary of Education | NATIONAL_EXEC |
| Secretary of Energy | NATIONAL_EXEC |
| Secretary of Health and Human Services | NATIONAL_EXEC |
| Secretary of Homeland Security | NATIONAL_EXEC |
| Secretary of Housing and Urban Development | NATIONAL_EXEC |
| Secretary of Labor | NATIONAL_EXEC |
| Secretary of State | NATIONAL_EXEC |
| Secretary of the Interior | NATIONAL_EXEC |
| Secretary of the Treasury | NATIONAL_EXEC |
| Secretary of Transportation | NATIONAL_EXEC |
| Secretary of Veterans Affairs | NATIONAL_EXEC |
| Administrator of the Small Business Administration | NATIONAL_EXEC |
| Ambassador to the United Nations | NATIONAL_EXEC |
| Chair of the Council of Economic Advisers | NATIONAL_EXEC |
| Director of National Intelligence | NATIONAL_EXEC |
| Director of the Office of Management and Budget | NATIONAL_EXEC |
| Director of the Office of Science and Technology Policy | NATIONAL_EXEC |
| EPA Administrator | NATIONAL_EXEC |
| Trade Representative | NATIONAL_EXEC |
| White House Chief of Staff | NATIONAL_EXEC |

## How to Add
```bash
# Via admin API (requires auth session cookie):
curl -X POST http://localhost:5050/essentials/admin/position-descriptions \
  -H "Content-Type: application/json" \
  -b "session=YOUR_SESSION_COOKIE" \
  -d '{"normalized_position_name": "Sheriff", "district_type": "COUNTY", "description": "The Sheriff is the chief law enforcement officer...", "source": "manual"}'

# Or direct SQL:
INSERT INTO essentials.position_descriptions (id, normalized_position_name, district_type, description, source)
VALUES (uuid_generate_v4(), 'Sheriff', 'COUNTY', 'The Sheriff is the chief law enforcement officer...', 'manual');
```
