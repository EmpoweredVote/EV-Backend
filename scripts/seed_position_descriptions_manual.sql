-- Seed 54 missing position descriptions for essentials.position_descriptions.
-- Idempotent: uses ON CONFLICT to skip rows that already exist.
-- Run with: psql "$DATABASE_URL" -f seed_position_descriptions_manual.sql

BEGIN;

-- ============================================================
-- LOCAL (4)
-- ============================================================

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Assessor',
  'COUNTY',
  'The County Assessor determines the assessed value of all taxable property within the county for the purpose of calculating property taxes. This elected official maintains property records, processes exemptions and exclusions, and ensures equitable valuations across the jurisdiction. The Assessor''s office conducts periodic reassessments and responds to appeals from property owners who dispute their valuations.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Sheriff',
  'COUNTY',
  'The County Sheriff is the chief law enforcement officer of the county, responsible for policing unincorporated areas, operating the county jail, and providing court security. This elected official also serves civil processes such as evictions and subpoenas. In many counties the Sheriff''s department provides contract policing services to smaller cities and manages search-and-rescue operations.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Council Member',
  'LOCAL',
  'A City Council Member serves on the legislative body of a municipal government, voting on local ordinances, land-use decisions, and the city budget. Council members represent a specific district or serve at-large, depending on the city''s charter. They also approve contracts, set local tax rates, and provide oversight of city departments and services.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Supervisor',
  'LOCAL',
  'A Supervisor serves on the governing board of a county or local jurisdiction, setting policy and approving budgets for county services such as public health, social services, and infrastructure. Supervisors typically represent geographic districts and act as both legislative and executive authorities in county government. They also oversee land-use planning and coordinate with state and federal agencies on programs administered at the county level.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

-- ============================================================
-- SCHOOL (1)
-- ============================================================

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Board Member',
  'SCHOOL',
  'A School Board Member serves on the elected governing body of a public school district, setting educational policy, approving budgets, and hiring the superintendent. Board members make decisions on curriculum standards, school facilities, and collective bargaining agreements with district employees. They represent the community''s interests in ensuring quality public education for all students in the district.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

-- ============================================================
-- STATE (21)
-- ============================================================

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Assembly Member',
  'STATE_LOWER',
  'A State Assembly Member serves in the lower chamber of the state legislature, drafting and voting on state laws, approving the state budget, and representing constituents in a specific district. Assembly members introduce legislation, serve on policy committees, and provide oversight of state agencies. They typically serve two-year terms and must stand for re-election more frequently than state senators.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Senator',
  'STATE_UPPER',
  'A State Senator serves in the upper chamber of the state legislature, drafting and voting on state laws and approving the state budget. State senators represent larger districts than assembly members and typically serve four-year terms. They confirm gubernatorial appointments, serve on policy committees, and provide oversight of executive branch agencies.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Attorney General',
  'STATE_EXEC',
  'The State Attorney General is the chief legal officer of the state, representing the state government in court and providing legal opinions to state agencies and the legislature. This elected official oversees the state Department of Justice, enforces consumer protection and antitrust laws, and prosecutes cases of statewide significance. The Attorney General also coordinates law enforcement efforts across the state.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Auditor',
  'STATE_EXEC',
  'The State Auditor conducts independent audits of state government agencies, programs, and expenditures to ensure public funds are spent effectively and in compliance with the law. This official identifies waste, fraud, and inefficiency in state operations and issues public reports with recommendations for improvement. The Auditor serves as a key accountability mechanism in state government.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Board of Equalization Member',
  'STATE_EXEC',
  'A Board of Equalization Member serves on the state body responsible for overseeing the administration of property tax assessments and ensuring uniform valuations across counties. In California, this elected board also administers certain excise taxes on alcohol, tobacco, and cannabis. Members represent multi-county districts and hear appeals from taxpayers and county assessors.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Chairman',
  'STATE_EXEC',
  'The Chairman leads a state board or commission, presiding over meetings, setting agendas, and coordinating the work of fellow commissioners. This position may be elected or appointed depending on the specific board. The Chairman represents the commission in dealings with the governor, legislature, and the public.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Commissioner of Insurance',
  'STATE_EXEC',
  'The Commissioner of Insurance regulates the state''s insurance industry, reviewing and approving insurance rates, licensing insurers and agents, and enforcing consumer protection laws related to insurance. This official ensures the financial solvency of insurance companies operating in the state and investigates complaints from policyholders. Depending on the state, this position may be elected or appointed by the governor.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Commissioner of Labor',
  'STATE_EXEC',
  'The Commissioner of Labor oversees the state''s labor department, enforcing workplace safety regulations, wage and hour laws, and workers'' compensation programs. This official mediates labor disputes, administers unemployment insurance, and publishes labor market data. The Commissioner works to protect the rights of workers while promoting a fair business environment.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Controller',
  'STATE_EXEC',
  'The State Controller serves as the chief fiscal officer of the state, responsible for accounting, auditing, and disbursing state funds. This elected official issues the state''s paychecks, monitors the state budget, and publishes financial reports on government spending. The Controller also sits on various state boards that oversee public employee pensions, bond financing, and tax policy.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Director of Agriculture',
  'STATE_EXEC',
  'The Director of Agriculture leads the state department of agriculture, overseeing food safety inspections, pesticide regulation, and agricultural marketing programs. This official supports the state''s farming industry through trade promotion, pest management, and conservation programs. The Director is typically appointed by the governor and works to balance agricultural productivity with environmental protection.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Director of Industrial Relations',
  'STATE_EXEC',
  'The Director of Industrial Relations heads the state agency responsible for enforcing labor laws, workplace safety standards, and workers'' compensation regulations. This appointed official oversees divisions that handle occupational safety, apprenticeship programs, and labor standards enforcement. The Director works to ensure safe and fair working conditions across industries in the state.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Director of Natural Resources',
  'STATE_EXEC',
  'The Director of Natural Resources leads the state agency responsible for managing public lands, water resources, wildlife, and mineral rights. This appointed official oversees conservation programs, issues permits for resource extraction, and enforces environmental regulations. The Director coordinates policies that balance economic development with the preservation of the state''s natural environment.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Insurance Commissioner',
  'STATE_EXEC',
  'The Insurance Commissioner regulates the insurance market within the state, approving or denying rate changes, licensing insurance companies, and protecting consumers from unfair practices. This elected official investigates fraud, ensures insurers maintain adequate reserves to pay claims, and enforces state insurance laws. The Commissioner plays a critical role after natural disasters in overseeing the claims process.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Public Utilities Commissioner',
  'STATE_EXEC',
  'A Public Utilities Commissioner serves on the state body that regulates investor-owned electric, gas, water, and telecommunications utilities. Commissioners set the rates consumers pay, review utility infrastructure investments, and enforce safety and reliability standards. They balance the interests of ratepayers, utility companies, and environmental goals in approving energy procurement and infrastructure plans.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary for Natural Resources',
  'STATE_EXEC',
  'The Secretary for Natural Resources serves as a cabinet-level official overseeing the state''s natural resources agency, which manages public lands, water, wildlife, and coastal resources. This appointed official coordinates policy across multiple departments including parks, fish and wildlife, water resources, and forestry. The Secretary advises the governor on environmental and conservation matters.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of Agriculture',
  'STATE_EXEC',
  'The state Secretary of Agriculture leads the department responsible for supporting the agricultural industry, enforcing food safety standards, and managing pest prevention programs. This official promotes agricultural trade, oversees farm subsidies and conservation programs, and regulates pesticide use. The Secretary is typically appointed by the governor and serves as the primary advocate for farmers and ranchers at the state level.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of Education',
  'STATE_EXEC',
  'The state Secretary of Education oversees the state''s education agency, setting policy for public K-12 schools and coordinating with higher education institutions. This appointed official manages the distribution of state education funding, implements academic standards, and monitors school performance. The Secretary advises the governor on education reform and works with school districts to improve student outcomes.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of State',
  'STATE_EXEC',
  'The state Secretary of State oversees elections, maintains official state records, and registers businesses and trademarks. This elected official certifies election results, manages voter registration databases, and ensures compliance with campaign finance disclosure laws. The Secretary of State also authenticates official documents and may serve in the line of gubernatorial succession.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Superintendent',
  'STATE_EXEC',
  'The State Superintendent of Public Instruction is the chief education official, overseeing the state department of education and implementing education policy for public K-12 schools. This official manages the distribution of state and federal education funds, sets curriculum frameworks, and monitors school accountability measures. Depending on the state, the Superintendent may be elected or appointed.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Treasurer',
  'STATE_EXEC',
  'The State Treasurer manages the investment of state funds, oversees the issuance of state bonds, and serves as the state''s banker. This elected official ensures that public money is invested safely and earns competitive returns, manages cash flow for state operations, and administers college savings and other public finance programs. The Treasurer typically sits on various state financing authorities and pension boards.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Utility Regulatory Commissioner',
  'STATE_EXEC',
  'A Utility Regulatory Commissioner serves on the state commission that regulates public utilities including electricity, natural gas, water, and telecommunications providers. Commissioners review rate increase requests, approve infrastructure projects, and enforce service quality standards. They work to ensure reliable utility service at fair prices while encouraging investment in clean energy and modern infrastructure.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

-- ============================================================
-- FEDERAL (28)
-- ============================================================

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Representative',
  'NATIONAL_LOWER',
  'A U.S. Representative serves in the House of Representatives, the lower chamber of Congress, representing a specific congressional district. Representatives draft and vote on federal legislation, approve the federal budget, and have the exclusive power to initiate revenue bills and impeach federal officials. They serve two-year terms, making them the most frequently elected federal officials and closely tied to their constituents'' concerns.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Senator',
  'NATIONAL_UPPER',
  'A U.S. Senator serves in the Senate, the upper chamber of Congress, representing an entire state. Senators draft and vote on federal legislation, confirm presidential appointments including judges and cabinet members, and ratify treaties. Each state elects two senators to six-year terms, with elections staggered so that roughly one-third of the Senate is up for election every two years.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'President',
  'NATIONAL_EXEC',
  'The President of the United States is the head of state, head of government, and commander-in-chief of the armed forces. The President signs or vetoes legislation passed by Congress, issues executive orders, negotiates treaties, and appoints federal judges, cabinet members, and ambassadors. Elected to a four-year term through the Electoral College, the President sets the national policy agenda and represents the country in foreign affairs.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Vice President',
  'NATIONAL_EXEC',
  'The Vice President of the United States serves as the President''s principal backup and presides over the U.S. Senate, casting tie-breaking votes. The Vice President is first in the presidential line of succession and often represents the administration in diplomatic and ceremonial functions. Elected on a joint ticket with the President to a four-year term, the Vice President also chairs the National Space Council and other advisory bodies as assigned.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Attorney General',
  'NATIONAL_EXEC',
  'The U.S. Attorney General is the head of the Department of Justice and the chief law enforcement officer of the federal government. This cabinet member oversees federal prosecutors, the FBI, the Bureau of Prisons, and the DEA, and advises the President on legal matters. The Attorney General enforces federal laws, represents the United States in court, and sets priorities for criminal justice and civil rights enforcement.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of Agriculture',
  'NATIONAL_EXEC',
  'The U.S. Secretary of Agriculture leads the Department of Agriculture (USDA), which oversees federal policies on farming, food safety, nutrition assistance, and rural development. This cabinet member manages programs including SNAP (food stamps), crop insurance, forest management, and agricultural research. The Secretary works to support American farmers, ensure a safe food supply, and promote conservation on agricultural lands.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of Commerce',
  'NATIONAL_EXEC',
  'The Secretary of Commerce leads the Department of Commerce, which promotes economic growth, job creation, and international trade. This cabinet member oversees the Census Bureau, the Patent and Trademark Office, NOAA, and the Bureau of Economic Analysis. The Secretary negotiates trade agreements, enforces export controls, and supports American businesses in competing in the global economy.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of Defense',
  'NATIONAL_EXEC',
  'The Secretary of Defense is the head of the Department of Defense and the principal defense policy advisor to the President. This cabinet member oversees the U.S. military, including the Army, Navy, Air Force, Marine Corps, and Space Force, as well as the Joint Chiefs of Staff. The Secretary manages the largest departmental budget in the federal government and directs military operations, weapons procurement, and defense strategy.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of Education',
  'NATIONAL_EXEC',
  'The Secretary of Education leads the Department of Education, which administers federal education funding, enforces civil rights laws in schools, and collects data on student achievement. This cabinet member manages federal student loan and grant programs, sets policy on K-12 accountability standards, and supports states in improving educational outcomes. The Secretary serves as the President''s chief advisor on education policy.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of Energy',
  'NATIONAL_EXEC',
  'The Secretary of Energy heads the Department of Energy, which oversees the nation''s nuclear weapons stockpile, energy research, and energy policy. This cabinet member manages the national laboratories, promotes renewable energy development, and sets standards for energy efficiency. The Secretary also oversees the cleanup of legacy nuclear waste sites and coordinates the strategic petroleum reserve.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of Health and Human Services',
  'NATIONAL_EXEC',
  'The Secretary of Health and Human Services (HHS) leads the department responsible for protecting public health and providing essential human services. This cabinet member oversees the CDC, FDA, NIH, Medicare, Medicaid, and the Administration for Children and Families. The Secretary coordinates the nation''s response to public health emergencies, regulates drugs and medical devices, and administers health insurance programs serving millions of Americans.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of Homeland Security',
  'NATIONAL_EXEC',
  'The Secretary of Homeland Security heads the Department of Homeland Security (DHS), which is responsible for border security, immigration enforcement, cybersecurity, and disaster response. This cabinet member oversees agencies including Customs and Border Protection, ICE, the Secret Service, TSA, FEMA, and the Coast Guard. The Secretary coordinates efforts to prevent terrorism, secure critical infrastructure, and respond to natural disasters.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of Housing and Urban Development',
  'NATIONAL_EXEC',
  'The Secretary of Housing and Urban Development (HUD) leads the department that oversees federal housing programs, fair housing enforcement, and community development initiatives. This cabinet member manages public housing, Section 8 vouchers, FHA mortgage insurance, and homelessness assistance programs. The Secretary works to expand affordable housing, combat housing discrimination, and revitalize distressed communities.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of Labor',
  'NATIONAL_EXEC',
  'The Secretary of Labor heads the Department of Labor, which enforces federal labor laws covering wages, workplace safety, unemployment insurance, and workers'' rights. This cabinet member oversees OSHA, the Bureau of Labor Statistics, and the Wage and Hour Division. The Secretary works to improve working conditions, promote job training and employment opportunities, and protect the rights of workers and retirees.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of State',
  'NATIONAL_EXEC',
  'The U.S. Secretary of State is the nation''s chief diplomat and head of the Department of State, responsible for conducting foreign policy and managing diplomatic relations with other countries. This cabinet member, fourth in the presidential line of succession, oversees U.S. embassies and consulates worldwide, negotiates international agreements, and advises the President on foreign affairs. The Secretary of State also manages passport and visa services for American citizens and foreign nationals.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of the Interior',
  'NATIONAL_EXEC',
  'The Secretary of the Interior leads the Department of the Interior, which manages and conserves federal lands, national parks, and natural resources. This cabinet member oversees the National Park Service, the Bureau of Land Management, the U.S. Fish and Wildlife Service, and the Bureau of Indian Affairs. The Secretary is responsible for protecting endangered species, managing energy and mineral resources on public lands, and honoring the federal government''s trust responsibilities to Native American tribes.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of the Treasury',
  'NATIONAL_EXEC',
  'The Secretary of the Treasury leads the Department of the Treasury, which manages federal finances, collects taxes through the IRS, and advises the President on economic policy. This cabinet member, second in the presidential line of succession among cabinet officers, oversees the minting of currency, manages the national debt, and enforces financial sanctions. The Secretary also represents the U.S. in international economic organizations and coordinates responses to financial crises.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of Transportation',
  'NATIONAL_EXEC',
  'The Secretary of Transportation heads the Department of Transportation, which oversees federal highway, aviation, railroad, maritime, and transit programs. This cabinet member manages agencies including the FAA, the Federal Highway Administration, and the National Highway Traffic Safety Administration. The Secretary sets safety standards for vehicles and transportation infrastructure, distributes federal transportation funding to states, and coordinates national transportation policy.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Secretary of Veterans Affairs',
  'NATIONAL_EXEC',
  'The Secretary of Veterans Affairs leads the Department of Veterans Affairs (VA), the second-largest federal department, which provides healthcare, disability benefits, education assistance, and home loan guarantees to military veterans. This cabinet member oversees the VA hospital system, manages the GI Bill education program, and administers veterans'' cemeteries. The Secretary works to ensure that the nation honors its commitments to those who served in the armed forces.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Administrator of the Small Business Administration',
  'NATIONAL_EXEC',
  'The Administrator of the Small Business Administration (SBA) leads the agency that supports small businesses through loan guarantees, disaster assistance, and entrepreneurial development programs. This cabinet-level official oversees programs that help Americans start and grow businesses, including mentoring networks and government contracting preferences for small firms. The Administrator also coordinates the federal response to help small businesses recover from natural disasters.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Ambassador to the United Nations',
  'NATIONAL_EXEC',
  'The U.S. Ambassador to the United Nations represents the United States at the UN, serving as the country''s chief delegate in the General Assembly and the Security Council. This cabinet-level official advocates for American foreign policy interests in multilateral negotiations on issues including international peace and security, human rights, and humanitarian assistance. The Ambassador coordinates with the Secretary of State and the National Security Council on UN-related matters.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Chair of the Council of Economic Advisers',
  'NATIONAL_EXEC',
  'The Chair of the Council of Economic Advisers (CEA) leads the three-member council that advises the President on economic policy and prepares the annual Economic Report of the President. This cabinet-level official provides data-driven analysis on employment, inflation, trade, and fiscal policy to inform White House decision-making. The Chair represents the administration''s economic views to Congress, the media, and the public.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Director of National Intelligence',
  'NATIONAL_EXEC',
  'The Director of National Intelligence (DNI) serves as the head of the U.S. Intelligence Community, coordinating the efforts of 18 intelligence agencies including the CIA, NSA, and FBI intelligence divisions. This cabinet-level official briefs the President on national security threats through the President''s Daily Brief and sets priorities for intelligence collection and analysis. The DNI oversees information sharing across agencies and ensures intelligence activities comply with the law.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Director of the Office of Management and Budget',
  'NATIONAL_EXEC',
  'The Director of the Office of Management and Budget (OMB) oversees the preparation of the President''s annual budget proposal and manages the performance of federal agencies. This cabinet-level official reviews all significant federal regulations before they are published, evaluates the effectiveness of government programs, and coordinates legislative proposals from executive agencies. The OMB Director plays a central role in setting the administration''s fiscal priorities.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Director of the Office of Science and Technology Policy',
  'NATIONAL_EXEC',
  'The Director of the Office of Science and Technology Policy (OSTP) serves as the President''s chief science advisor, coordinating federal science, technology, and innovation policy. This official leads efforts on issues including artificial intelligence, climate science, biotechnology, and STEM education. The Director co-chairs the National Science and Technology Council and advises the President on the scientific aspects of domestic and international policy.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'EPA Administrator',
  'NATIONAL_EXEC',
  'The EPA Administrator heads the Environmental Protection Agency, which is responsible for protecting human health and the environment by enforcing federal environmental laws. This cabinet-level official sets standards for air and water quality, regulates hazardous waste disposal, and oversees the cleanup of contaminated sites. The Administrator manages programs addressing climate change, chemical safety, and environmental justice across all 50 states and U.S. territories.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'Trade Representative',
  'NATIONAL_EXEC',
  'The U.S. Trade Representative (USTR) is the President''s principal trade advisor and chief trade negotiator, holding the rank of Ambassador. This cabinet-level official negotiates bilateral and multilateral trade agreements, resolves trade disputes at the World Trade Organization, and coordinates trade policy across federal agencies. The Trade Representative works to open foreign markets to American goods and services while enforcing U.S. trade laws.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

INSERT INTO essentials.position_descriptions
  (id, normalized_position_name, district_type, description, source)
VALUES (
  uuid_generate_v4(),
  'White House Chief of Staff',
  'NATIONAL_EXEC',
  'The White House Chief of Staff is the President''s senior advisor and the highest-ranking member of the White House staff, managing the daily operations of the West Wing. This official controls access to the President, coordinates policy development across cabinet agencies, and serves as a key liaison between the White House and Congress. The Chief of Staff plays a central role in setting the administration''s strategic priorities and managing political relationships.',
  'manual'
) ON CONFLICT (normalized_position_name, district_type) DO NOTHING;

COMMIT;

-- Verify results
SELECT
  district_type,
  COUNT(*) AS count
FROM essentials.position_descriptions
GROUP BY district_type
ORDER BY district_type;
