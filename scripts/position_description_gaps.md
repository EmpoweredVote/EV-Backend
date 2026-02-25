# Position Description Gaps

All known gaps have been filled. See `seed_position_descriptions_manual.sql` for the 54 descriptions added manually.

## Status: Complete (58 total seeded)

### From BallotReady data (4) — `seed_position_descriptions.py`
- Mayor (LOCAL_EXEC)
- District Attorney (COUNTY)
- Governor (STATE_EXEC)
- Lieutenant Governor (STATE_EXEC)

### Manual descriptions (54) — `seed_position_descriptions_manual.sql`

**Local (4):** Assessor (COUNTY), Sheriff (COUNTY), Council Member (LOCAL), Supervisor (LOCAL)

**School (1):** Board Member (SCHOOL)

**State (21):** Assembly Member (STATE_LOWER), Senator (STATE_UPPER), Attorney General, Auditor, Board of Equalization Member, Chairman, Commissioner of Insurance, Commissioner of Labor, Controller, Director of Agriculture, Director of Industrial Relations, Director of Natural Resources, Insurance Commissioner, Public Utilities Commissioner, Secretary for Natural Resources, Secretary of Agriculture, Secretary of Education, Secretary of State, Superintendent, Treasurer, Utility Regulatory Commissioner (all STATE_EXEC unless noted)

**Federal (28):** President, Vice President, Attorney General, Secretary of Agriculture, Secretary of Commerce, Secretary of Defense, Secretary of Education, Secretary of Energy, Secretary of Health and Human Services, Secretary of Homeland Security, Secretary of Housing and Urban Development, Secretary of Labor, Secretary of State, Secretary of the Interior, Secretary of the Treasury, Secretary of Transportation, Secretary of Veterans Affairs, Administrator of the Small Business Administration, Ambassador to the United Nations, Chair of the Council of Economic Advisers, Director of National Intelligence, Director of the Office of Management and Budget, Director of the Office of Science and Technology Policy, EPA Administrator, Trade Representative, White House Chief of Staff (all NATIONAL_EXEC), Representative (NATIONAL_LOWER), Senator (NATIONAL_UPPER)
