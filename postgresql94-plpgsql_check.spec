%global pgmajorversion 94
%global pginstdir /usr/pgsql-9.4
%global sname plpgsql_check

Name:		%{sname}_%{pgmajorversion}
Version:	1.2.2
Release:	1%{?dist}
Summary:	Additional tools for plpgsql functions validation

Group:		Applications/Databases
License:	BSD
URL:		https://github.com/okbob/plpgsql_check/archive/v%{version}.zip
Source0:	plpgsql_check-%{version}.zip
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:	postgresql%{pgmajorversion}-devel
Requires:	postgresql%{pgmajorversion}

%description
The plpgsql_check is PostgreSQL extension with functionality for direct
or indirect extra validation of functions in plpgsql language. It verifies
a validity of SQL identifiers used in plpgsql code. It try to identify
a performance issues.

%prep
%setup -q -n %{sname}-%{version}


%build
PATH="%{pginstdir}/bin;$PATH" ; export PATH
CFLAGS="${CFLAGS:-%optflags}" ; export CFLAGS

USE_PGXS=1 make %{?_smp_mflags}

%install
rm -rf %{buildroot}
make USE_PGXS=1 DESTDIR=%{buildroot} install

%clean
rm -rf %{buildroot}

%files
%defattr(644,root,root,755)
%doc README.md
%{pginstdir}/lib/plpgsql_check.so
%{pginstdir}/share/extension/plpgsql_check--1.2.sql
%{pginstdir}/share/extension/plpgsql_check--1.0--1.1.sql
%{pginstdir}/share/extension/plpgsql_check--1.1--1.2.sql
%{pginstdir}/share/extension/plpgsql_check.control

%changelog
* Thu Oct 26 2017 - Pavel STEHULE <pavel.stehule@gmail.com> 1.2.2-1
- never read variables detection
- fix false alarm on MOVE command

* Fri Sep 15 2017 - Pavel STEHULE <pavel.stehule@gmail.com> 1.2.1-1
- missing RETURN detection
- fix some bugs and false alarms
- PostgreSQL 11 support

* Fri Now 11 2016 - Pavel STEHULE <pavel.stehule@gmail.com> 1.2.0-1
- support extra warnings - shadowed variables

* Thu Aug 25 2016 - Pavel STEHULE <pavel.stehule@gmail.com> 1.0.5-1
- minor fixes, support for PostgreSQL 10

* Fri Apr 15 2016 - Pavel STEHULE <pavel.stehule@gmail.com> 1.0.4-1
- support for PostgreSQL 9.6

* Mon Oct 12 2015 - Pavel STEHULE <pavel.stehule@gmail.com> 1.0.3-1
- fix false alarms of unused cursor variables
- fix regress tests

* Thu Jul 09 2015 - Pavel STEHULE <pavel.stehule@gmail.com> 1.0.2-2
- bugfix release

* Fri Dec 19 2014 - Pavel STEHULE <pavel.stehule@gmail.com> 0.9.3-1
- fix a broken record field type checking
- add check for assign to array field

* Mon Aug 25 2014 - Pavel STEHULE <pavel.stehule@gmail.com> 0.9.1-1
- Initial packaging
